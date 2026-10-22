package manager

import (
	"encoding/json"

	"github.com/cloudreve/Cloudreve/v4/pkg/cache"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
)

const (
	// UploadReceiptCachePrefix 已完成上传会话的回执。会话完成后 `callback_<id>` 立即删除，
	// 完成响应丢失的客户端重试时只会得到 40011；回执让它能向服务端确认“这个会话已经完成”，
	// 而不是重建会话（得到 40004 冲突或重复上传一个版本）。
	UploadReceiptCachePrefix = "upload_receipt_"
	// UploadReceiptTTL 回执保留时间（秒）。超过后客户端按会话已失效处理（与旧版本行为相同）。
	UploadReceiptTTL = 7 * 24 * 3600
)

// UploadReceipt 上传会话完成回执。KV 中保存 JSON 字符串，内存 KV 落盘（gob）时无需注册类型。
type UploadReceipt struct {
	UID int    `json:"uid"`
	Uri string `json:"uri"`
}

// recordUploadReceipt 记录会话已完成；只在主机（有 DBFS）完成上传时调用。
func recordUploadReceipt(kv cache.Driver, session *fs.UploadSession) error {
	uri := ""
	if session.Props.Uri != nil {
		uri = session.Props.Uri.String()
	}
	raw, err := json.Marshal(UploadReceipt{UID: session.UID, Uri: uri})
	if err != nil {
		return err
	}
	return kv.Set(UploadReceiptCachePrefix+session.Props.UploadSessionID, string(raw), UploadReceiptTTL)
}

// GetUploadReceipt 读取会话 [sessionID] 的完成回执。
func GetUploadReceipt(kv cache.Driver, sessionID string) (*UploadReceipt, bool) {
	if sessionID == "" {
		return nil, false
	}
	raw, ok := kv.Get(UploadReceiptCachePrefix + sessionID)
	if !ok {
		return nil, false
	}
	s, ok := raw.(string)
	if !ok {
		return nil, false
	}
	var r UploadReceipt
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		return nil, false
	}
	return &r, true
}
