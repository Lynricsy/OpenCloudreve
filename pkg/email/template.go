package email

import (
	"context"
	"fmt"
	"html/template"
	"net/url"
	"strings"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
)

type CommonContext struct {
	SiteBasic *setting.SiteBasic
	Logo      *setting.Logo
	SiteUrl   string
}

// ResetContext used for variables in reset email
type ResetContext struct {
	*CommonContext
	User *ent.User
	Url  string
}

// NewResetEmail generates reset email from template
func NewResetEmail(ctx context.Context, settings setting.Provider, user *ent.User, url string) (string, string, error) {
	templates := settings.ResetEmailTemplate(ctx)
	if len(templates) == 0 {
		return "", "", fmt.Errorf("reset email template not configured")
	}

	selected := selectTemplate(templates, user)
	resetCtx := ResetContext{
		CommonContext: commonContext(ctx, settings),
		User:          user,
		Url:           url,
	}

	tmplTitle, err := template.New("resetTitle").Parse(selected.Title)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse email title: %w", err)
	}

	var resTitle strings.Builder
	err = tmplTitle.Execute(&resTitle, resetCtx.templateData())
	if err != nil {
		return "", "", fmt.Errorf("failed to execute email title: %w", err)
	}

	tmplBody, err := template.New("resetBody").Parse(selected.Body)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse email template: %w", err)
	}

	var resBody strings.Builder
	err = tmplBody.Execute(&resBody, resetCtx.templateData())
	if err != nil {
		return "", "", fmt.Errorf("failed to execute email template: %w", err)
	}

	return resTitle.String(), resBody.String(), nil
}

// ActivationContext used for variables in activation email
type ActivationContext struct {
	*CommonContext
	User *ent.User
	Url  string
}

// NewActivationEmail generates activation email from template
func NewActivationEmail(ctx context.Context, settings setting.Provider, user *ent.User, url string) (string, string, error) {
	templates := settings.ActivationEmailTemplate(ctx)
	if len(templates) == 0 {
		return "", "", fmt.Errorf("activation email template not configured")
	}

	selected := selectTemplate(templates, user)
	activationCtx := ActivationContext{
		CommonContext: commonContext(ctx, settings),
		User:          user,
		Url:           url,
	}

	tmplTitle, err := template.New("activationTitle").Parse(selected.Title)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse email title: %w", err)
	}

	var resTitle strings.Builder
	err = tmplTitle.Execute(&resTitle, activationCtx.templateData())
	if err != nil {
		return "", "", fmt.Errorf("failed to execute email title: %w", err)
	}

	tmplBody, err := template.New("activationBody").Parse(selected.Body)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse email template: %w", err)
	}

	var resBody strings.Builder
	err = tmplBody.Execute(&resBody, activationCtx.templateData())
	if err != nil {
		return "", "", fmt.Errorf("failed to execute email template: %w", err)
	}

	return resTitle.String(), resBody.String(), nil
}

// ExceedQuotaContext 存储配额超出通知邮件的模板变量
type ExceedQuotaContext struct {
	*CommonContext
	User  *ent.User
	Used  string
	Total string
}

// NewExceedQuotaEmail 根据模板生成存储配额超出通知邮件，used/total 为字节数，渲染时格式化为可读容量。
func NewExceedQuotaEmail(ctx context.Context, settings setting.Provider, user *ent.User, used, total int64) (string, string, error) {
	templates := settings.ExceedQuotaEmailTemplate(ctx)
	if len(templates) == 0 {
		return "", "", fmt.Errorf("exceed quota email template not configured")
	}

	selected := selectTemplate(templates, user)
	quotaCtx := ExceedQuotaContext{
		CommonContext: commonContext(ctx, settings),
		User:          user,
		Used:          formatSize(used),
		Total:         formatSize(total),
	}

	tmplTitle, err := template.New("exceedQuotaTitle").Parse(selected.Title)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse email title: %w", err)
	}

	var resTitle strings.Builder
	err = tmplTitle.Execute(&resTitle, quotaCtx.templateData())
	if err != nil {
		return "", "", fmt.Errorf("failed to execute email title: %w", err)
	}

	tmplBody, err := template.New("exceedQuotaBody").Parse(selected.Body)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse email template: %w", err)
	}

	var resBody strings.Builder
	err = tmplBody.Execute(&resBody, quotaCtx.templateData())
	if err != nil {
		return "", "", fmt.Errorf("failed to execute email template: %w", err)
	}

	return resTitle.String(), resBody.String(), nil
}

// formatSize 将字节数格式化为以 1024 为进制的可读容量，例如 "1.5 GB"。
func formatSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}

	units := []string{"KB", "MB", "GB", "TB", "PB", "EB"}
	value := float64(size) / unit
	i := 0
	for value >= unit && i < len(units)-1 {
		value /= unit
		i++
	}

	return strings.TrimSuffix(fmt.Sprintf("%.2f", value), ".00") + " " + units[i]
}

func commonContext(ctx context.Context, settings setting.Provider) *CommonContext {
	logo := settings.Logo(ctx)
	siteUrl := settings.SiteURL(ctx)
	resolvedLogo := *logo

	// Add site url if logo is not an url
	if !strings.HasPrefix(logo.Light, "http") {
		logoPath, _ := url.Parse(logo.Light)
		resolvedLogo.Light = siteUrl.ResolveReference(logoPath).String()
	}

	if !strings.HasPrefix(logo.Normal, "http") {
		logoPath, _ := url.Parse(logo.Normal)
		resolvedLogo.Normal = siteUrl.ResolveReference(logoPath).String()
	}

	return &CommonContext{
		SiteBasic: settings.SiteBasic(ctx),
		Logo:      &resolvedLogo,
		SiteUrl:   siteUrl.String(),
	}
}

func (c *CommonContext) templateData() map[string]any {
	return map[string]any{
		"SiteBasic": map[string]string{
			"Name":        c.SiteBasic.Name,
			"Title":       c.SiteBasic.Title,
			"ID":          c.SiteBasic.ID,
			"Description": c.SiteBasic.Description,
			"Script":      c.SiteBasic.Script,
		},
		"Logo": map[string]string{
			"Normal": c.Logo.Normal,
			"Light":  c.Logo.Light,
		},
		"SiteUrl": c.SiteUrl,
	}
}

func (c ResetContext) templateData() map[string]any {
	return map[string]any{
		"CommonContext": c.CommonContext.templateData(),
		"User":          c.User,
		"Url":           c.Url,
	}
}

func (c ActivationContext) templateData() map[string]any {
	return map[string]any{
		"CommonContext": c.CommonContext.templateData(),
		"User":          c.User,
		"Url":           c.Url,
	}
}

func (c ExceedQuotaContext) templateData() map[string]any {
	return map[string]any{
		"CommonContext": c.CommonContext.templateData(),
		"User":          c.User,
		"Used":          c.Used,
		"Total":         c.Total,
	}
}

func selectTemplate(templates []setting.EmailTemplate, u *ent.User) setting.EmailTemplate {
	selected := templates[0]
	if u != nil {
		for _, t := range templates {
			if strings.EqualFold(t.Language, u.Settings.Language) {
				selected = t
				break
			}
		}
	}

	return selected
}
