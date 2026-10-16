package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	googleAuthURL  = "https://accounts.google.com/o/oauth2/v2/auth"
	googleTokenURL = "https://oauth2.googleapis.com/token"
	driveAPI       = "https://www.googleapis.com/drive/v3"
	driveUploadAPI = "https://www.googleapis.com/upload/drive/v3"
	// DriveScope 只能访问本应用创建的文件，属于 Google 的非敏感权限范围。
	DriveScope = "https://www.googleapis.com/auth/drive.file"
	folderMime = "application/vnd.google-apps.folder"

	driveMaxRetries = 6
	// tokenExpiryMargin access token 提前刷新的余量。
	tokenExpiryMargin = 5 * time.Minute
)

var (
	// uploadChunkSize 断点续传的分块大小，必须是 256 KiB 的整数倍（测试中会调小）。
	uploadChunkSize = 16 << 20
	// retryBaseDelay 重试退避的基础间隔（测试中会调小）。
	retryBaseDelay = time.Second
)

// ErrDriveUnauthorized 刷新令牌失效（被撤销、过期或 OAuth 客户端变更），需要管理员重新授权。
var ErrDriveUnauthorized = errors.New("google drive authorization is invalid, please sign in again")

type (
	// DriveFile Drive 文件元数据（只请求需要的字段）。
	DriveFile struct {
		ID            string            `json:"id"`
		Name          string            `json:"name"`
		MimeType      string            `json:"mimeType,omitempty"`
		Size          string            `json:"size,omitempty"`
		CreatedTime   time.Time         `json:"createdTime,omitempty"`
		Trashed       bool              `json:"trashed,omitempty"`
		Parents       []string          `json:"parents,omitempty"`
		AppProperties map[string]string `json:"appProperties,omitempty"`
	}

	// DriveAbout 账号与空间信息。
	DriveAbout struct {
		User struct {
			DisplayName  string `json:"displayName"`
			EmailAddress string `json:"emailAddress"`
		} `json:"user"`
		StorageQuota struct {
			Limit string `json:"limit"`
			Usage string `json:"usage"`
		} `json:"storageQuota"`
	}

	// DriveError Drive / OAuth 接口返回的错误。
	DriveError struct {
		Status  int
		Reason  string
		Message string
	}

	// TokenSource 提供 Drive 访问令牌。
	TokenSource interface {
		Token(ctx context.Context) (string, error)
	}

	// DriveClient 基于 Drive v3 REST 接口的最小客户端。
	DriveClient struct {
		http   *http.Client
		tokens TokenSource
	}

	tokenResponse struct {
		AccessToken      string `json:"access_token"`
		ExpiresIn        int64  `json:"expires_in"`
		RefreshToken     string `json:"refresh_token"`
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}

	// OAuthApp Google OAuth 客户端配置。
	OAuthApp struct {
		ClientID     string
		ClientSecret string
	}
)

func (e *DriveError) Error() string {
	return fmt.Sprintf("google drive: http %d %s: %s", e.Status, e.Reason, e.Message)
}

// retryable 限流、服务端错误可重试。
func (e *DriveError) retryable() bool {
	if e.Status == http.StatusTooManyRequests || e.Status >= 500 {
		return true
	}
	return e.Status == http.StatusForbidden && (e.Reason == "rateLimitExceeded" || e.Reason == "userRateLimitExceeded")
}

// NewHTTPClient 创建访问 Google 的 HTTP 客户端；proxy 为空时使用环境变量中的代理。
func NewHTTPClient(proxy string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if proxy != "" {
		u, err := url.Parse(proxy)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy %q: %w", proxy, err)
		}
		transport.Proxy = http.ProxyURL(u)
	}
	// 不设整体超时：上传分块与下载可能耗时较长，由 ctx 控制取消。
	return &http.Client{Transport: transport}, nil
}

// AuthURL 生成 Google 授权页面地址。prompt=consent 保证每次都返回刷新令牌。
func (a OAuthApp) AuthURL(redirect, state string) string {
	q := url.Values{
		"client_id":     {a.ClientID},
		"redirect_uri":  {redirect},
		"response_type": {"code"},
		"scope":         {DriveScope},
		"access_type":   {"offline"},
		"prompt":        {"consent"},
		"state":         {state},
	}
	return googleAuthURL + "?" + q.Encode()
}

// ExchangeCode 用授权码换取刷新令牌。
func (a OAuthApp) ExchangeCode(ctx context.Context, hc *http.Client, redirect, code string) (string, error) {
	res, err := a.token(ctx, hc, url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {redirect},
	})
	if err != nil {
		return "", err
	}
	if res.RefreshToken == "" {
		return "", errors.New("google did not return a refresh token")
	}
	return res.RefreshToken, nil
}

func (a OAuthApp) token(ctx context.Context, hc *http.Client, form url.Values) (*tokenResponse, error) {
	form.Set("client_id", a.ClientID)
	form.Set("client_secret", a.ClientSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, googleTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to request google token: %w", err)
	}
	defer resp.Body.Close()

	res := &tokenResponse{}
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, fmt.Errorf("failed to decode google token response (http %d): %w", resp.StatusCode, err)
	}
	if res.Error != "" {
		if res.Error == "invalid_grant" || res.Error == "invalid_client" || res.Error == "unauthorized_client" {
			return nil, fmt.Errorf("%w: %s %s", ErrDriveUnauthorized, res.Error, res.ErrorDescription)
		}
		return nil, &DriveError{Status: resp.StatusCode, Reason: res.Error, Message: res.ErrorDescription}
	}
	return res, nil
}

// Refresh 用刷新令牌换取 access token，返回令牌与（已扣除余量的）过期时间。
func (a OAuthApp) Refresh(ctx context.Context, hc *http.Client, refreshToken string) (string, time.Time, error) {
	if refreshToken == "" {
		return "", time.Time{}, ErrDriveUnauthorized
	}
	res, err := a.token(ctx, hc, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return res.AccessToken, time.Now().Add(time.Duration(res.ExpiresIn)*time.Second - tokenExpiryMargin), nil
}

// NewDriveClient 创建 Drive 客户端。
func NewDriveClient(hc *http.Client, tokens TokenSource) *DriveClient {
	return &DriveClient{http: hc, tokens: tokens}
}

// quoteQuery 转义 Drive 查询语句中的字符串字面量。
func quoteQuery(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return "'" + strings.ReplaceAll(s, `'`, `\'`) + "'"
}

func decodeDriveError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var payload struct {
		Error struct {
			Message string `json:"message"`
			Errors  []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
			Status string `json:"status"`
		} `json:"error"`
	}
	de := &DriveError{Status: resp.StatusCode, Message: strings.TrimSpace(string(body))}
	if json.Unmarshal(body, &payload) == nil && payload.Error.Message != "" {
		de.Message = payload.Error.Message
		de.Reason = payload.Error.Status
		if len(payload.Error.Errors) > 0 {
			de.Reason = payload.Error.Errors[0].Reason
		}
	}
	return de
}

func backoff(ctx context.Context, attempt int) error {
	d := time.Duration(1<<attempt) * retryBaseDelay
	if d > 32*retryBaseDelay {
		d = 32 * retryBaseDelay
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func isRetryable(err error) bool {
	var de *DriveError
	if errors.As(err, &de) {
		return de.retryable()
	}
	// 网络错误（连接重置、超时等）可重试；ctx 取消不可重试。
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrDriveUnauthorized)
}

// do 发送可重放的请求（body 为字节切片），对可重试错误按指数退避重试。成功时返回未关闭的响应。
func (c *DriveClient) do(ctx context.Context, method, rawURL string, header http.Header, body []byte) (*http.Response, error) {
	var lastErr error
	for attempt := range driveMaxRetries {
		if attempt > 0 {
			if err := backoff(ctx, attempt); err != nil {
				return nil, err
			}
		}
		token, err := c.tokens.Token(ctx)
		if err != nil {
			return nil, err
		}
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
		if err != nil {
			return nil, err
		}
		for k, v := range header {
			req.Header[k] = v
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			if !isRetryable(err) {
				return nil, err
			}
			continue
		}
		if resp.StatusCode < 300 {
			return resp, nil
		}
		lastErr = decodeDriveError(resp)
		resp.Body.Close()
		if !isRetryable(lastErr) {
			return nil, lastErr
		}
	}
	return nil, lastErr
}

func (c *DriveClient) doJSON(ctx context.Context, method, rawURL string, in, out any) error {
	var (
		body   []byte
		header = http.Header{}
	)
	if in != nil {
		var err error
		if body, err = json.Marshal(in); err != nil {
			return err
		}
		header.Set("Content-Type", "application/json; charset=UTF-8")
	}
	resp, err := c.do(ctx, method, rawURL, header, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

const driveFileFields = "id,name,mimeType,size,createdTime,trashed,parents,appProperties"

// About 获取账号与空间信息。
func (c *DriveClient) About(ctx context.Context) (*DriveAbout, error) {
	res := &DriveAbout{}
	err := c.doJSON(ctx, http.MethodGet, driveAPI+"/about?fields=user(displayName,emailAddress),storageQuota(limit,usage)", nil, res)
	return res, err
}

// Get 获取文件元数据。
func (c *DriveClient) Get(ctx context.Context, id string) (*DriveFile, error) {
	res := &DriveFile{}
	err := c.doJSON(ctx, http.MethodGet, driveAPI+"/files/"+url.PathEscape(id)+"?fields="+driveFileFields, nil, res)
	return res, err
}

// List 按查询语句分页列出文件，对每个文件调用 fn。
func (c *DriveClient) List(ctx context.Context, q string, fn func(*DriveFile) error) error {
	pageToken := ""
	for {
		params := url.Values{
			"q":        {q},
			"fields":   {"nextPageToken,files(" + driveFileFields + ")"},
			"pageSize": {"1000"},
			"spaces":   {"drive"},
		}
		if pageToken != "" {
			params.Set("pageToken", pageToken)
		}
		var page struct {
			NextPageToken string       `json:"nextPageToken"`
			Files         []*DriveFile `json:"files"`
		}
		if err := c.doJSON(ctx, http.MethodGet, driveAPI+"/files?"+params.Encode(), nil, &page); err != nil {
			return err
		}
		for _, f := range page.Files {
			if err := fn(f); err != nil {
				return err
			}
		}
		if page.NextPageToken == "" {
			return nil
		}
		pageToken = page.NextPageToken
	}
}

// CreateFolder 创建目录。
func (c *DriveClient) CreateFolder(ctx context.Context, name, parent string, props map[string]string) (*DriveFile, error) {
	meta := &DriveFile{Name: name, MimeType: folderMime, AppProperties: props}
	if parent != "" {
		meta.Parents = []string{parent}
	}
	res := &DriveFile{}
	err := c.doJSON(ctx, http.MethodPost, driveAPI+"/files?fields="+driveFileFields, meta, res)
	return res, err
}

// Delete 永久删除文件；文件已不存在视为成功。
func (c *DriveClient) Delete(ctx context.Context, id string) error {
	err := c.doJSON(ctx, http.MethodDelete, driveAPI+"/files/"+url.PathEscape(id), nil, nil)
	var de *DriveError
	if errors.As(err, &de) && de.Status == http.StatusNotFound {
		return nil
	}
	return err
}

// Download 下载文件内容，调用方负责关闭。
func (c *DriveClient) Download(ctx context.Context, id string) (io.ReadCloser, error) {
	resp, err := c.do(ctx, http.MethodGet, driveAPI+"/files/"+url.PathEscape(id)+"?alt=media", nil, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Upload 以断点续传方式上传 r 的全部内容（长度事先未知）。每个分块失败后会查询服务端已接收的偏移并续传。
func (c *DriveClient) Upload(ctx context.Context, name, parent string, props map[string]string, r io.Reader) (*DriveFile, error) {
	meta, err := json.Marshal(&DriveFile{Name: name, Parents: []string{parent}, AppProperties: props})
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodPost, driveUploadAPI+"/files?uploadType=resumable&fields="+driveFileFields,
		http.Header{"Content-Type": {"application/json; charset=UTF-8"}}, meta)
	if err != nil {
		return nil, fmt.Errorf("failed to start resumable upload: %w", err)
	}
	resp.Body.Close()
	session := resp.Header.Get("Location")
	if session == "" {
		return nil, errors.New("google drive did not return an upload session")
	}

	buf := make([]byte, uploadChunkSize)
	var offset int64
	for {
		n, rerr := io.ReadFull(r, buf)
		final := false
		switch {
		case rerr == io.EOF || errors.Is(rerr, io.ErrUnexpectedEOF):
			final = true
		case rerr != nil:
			return nil, fmt.Errorf("failed to read upload source: %w", rerr)
		}
		f, err := c.putChunk(ctx, session, buf[:n], offset, final)
		if err != nil {
			return nil, err
		}
		offset += int64(n)
		if final {
			return f, nil
		}
	}
}

// putChunk 发送一个分块。非最终分块成功时返回 (nil, nil)，最终分块返回创建的文件。
func (c *DriveClient) putChunk(ctx context.Context, session string, data []byte, offset int64, final bool) (*DriveFile, error) {
	total := "*"
	if final {
		total = strconv.FormatInt(offset+int64(len(data)), 10)
	}
	end := offset + int64(len(data))
	sent := int64(0)
	queryOnly := false
	var lastErr error
	for attempt := 0; attempt < driveMaxRetries; {
		var (
			body io.Reader
			rng  string
			size int64
		)
		part := data[sent:]
		switch {
		case queryOnly:
			rng = "bytes */" + total
		case len(part) == 0:
			rng = "bytes */" + total
		default:
			body = bytes.NewReader(part)
			size = int64(len(part))
			rng = fmt.Sprintf("bytes %d-%d/%s", offset+sent, end-1, total)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, session, body)
		if err != nil {
			return nil, err
		}
		req.ContentLength = size
		req.Header.Set("Content-Range", rng)
		resp, err := c.http.Do(req)
		if err == nil {
			switch {
			case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
				defer resp.Body.Close()
				if !final {
					return nil, errors.New("google drive finished the upload before the last chunk")
				}
				f := &DriveFile{}
				if err := json.NewDecoder(resp.Body).Decode(f); err != nil {
					return nil, fmt.Errorf("failed to decode uploaded file: %w", err)
				}
				return f, nil
			case resp.StatusCode == 308:
				resp.Body.Close()
				acked := int64(0)
				if r := resp.Header.Get("Range"); r != "" {
					if i := strings.LastIndex(r, "-"); i >= 0 {
						if v, perr := strconv.ParseInt(r[i+1:], 10, 64); perr == nil {
							acked = v + 1
						}
					}
				}
				if acked < offset || acked > end {
					return nil, fmt.Errorf("unexpected upload offset %d, expected within [%d, %d]", acked, offset, end)
				}
				if acked == end && !final {
					return nil, nil
				}
				// 服务端只收到部分数据（或刚完成状态查询），从已确认的位置继续发送。
				sent = acked - offset
				queryOnly = false
				continue
			case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone:
				resp.Body.Close()
				return nil, errors.New("google drive upload session expired")
			default:
				lastErr = decodeDriveError(resp)
				resp.Body.Close()
				if !isRetryable(lastErr) {
					return nil, lastErr
				}
			}
		} else {
			lastErr = err
			if !isRetryable(err) {
				return nil, err
			}
		}
		// 失败：退避后先查询服务端已接收的偏移。
		attempt++
		if err := backoff(ctx, attempt); err != nil {
			return nil, err
		}
		queryOnly = true
	}
	return nil, fmt.Errorf("upload chunk failed after retries: %w", lastErr)
}
