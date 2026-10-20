package controllers

import (
	"fmt"
	"strings"

	"github.com/cloudreve/Cloudreve/v4/application/constants"
	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/cloudreve/Cloudreve/v4/service/basic"
	"github.com/gin-gonic/gin"
)

// SiteConfig 获取站点全局配置
func SiteConfig(c *gin.Context) {
	service := ParametersFromContext[*basic.GetSettingService](c, basic.GetSettingParamCtx{})

	resp, err := service.GetSiteConfig(c)
	if err != nil {
		c.JSON(200, serializer.Err(c, err))
		c.Abort()
		return
	}

	c.JSON(200, serializer.Response{
		Data: resp,
	})
}

// Ping 状态检查页面
func Ping(c *gin.Context) {
	c.JSON(200, serializer.Response{
		Code: 0,
		Data: constants.BackendVersion,
	})
}

// Captcha 获取验证码
func Captcha(c *gin.Context) {
	c.JSON(200, serializer.Response{
		Code: 0,
		Data: basic.GetCaptchaImage(c),
	})
}

// Manifest 获取manifest.json
func Manifest(c *gin.Context) {
	settingClient := dependency.FromContext(c).SettingProvider()
	siteOpts := settingClient.SiteBasic(c)
	pwaOpts := settingClient.PWA(c)
	c.Header("Cache-Control", "public, no-cache")
	c.JSON(200, map[string]interface{}{
		"short_name": siteOpts.Name,
		"name":       siteOpts.Name,
		"icons": []map[string]string{
			{
				"src":   pwaOpts.SmallIcon,
				"sizes": "64x64 32x32 24x24 16x16",
				"type":  "image/x-icon",
			},
			{
				"src":   pwaOpts.MediumIcon,
				"type":  "image/png",
				"sizes": "192x192",
			},
			{
				"src":   pwaOpts.LargeIcon,
				"type":  "image/png",
				"sizes": "512x512",
			},
		},
		"start_url":        ".",
		"display":          pwaOpts.Display,
		"theme_color":      pwaOpts.ThemeColor,
		"background_color": pwaOpts.BackgroundColor,
	})
}

// AssetLinks 输出 Android 数字资产链接（/.well-known/assetlinks.json），
// 让 Android 客户端的 Passkey（Credential Manager）与链接校验信任本站。未配置指纹时返回空数组。
func AssetLinks(c *gin.Context) {
	mobile := dependency.FromContext(c).SettingProvider().MobileApp(c)
	hashes := mobile.AndroidCertHashes()
	c.Header("Cache-Control", "public, no-cache")
	if mobile.AndroidPackage == "" || len(hashes) == 0 {
		c.JSON(200, []any{})
		return
	}

	// 统一输出为大写冒号分隔的十六进制
	fingerprints := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		parts := make([]string, len(hash))
		for i, b := range hash {
			parts[i] = fmt.Sprintf("%02X", b)
		}
		fingerprints = append(fingerprints, strings.Join(parts, ":"))
	}

	c.JSON(200, []map[string]any{
		{
			"relation": []string{
				"delegate_permission/common.handle_all_urls",
				"delegate_permission/common.get_login_creds",
			},
			"target": map[string]any{
				"namespace":                "android_app",
				"package_name":             mobile.AndroidPackage,
				"sha256_cert_fingerprints": fingerprints,
			},
		},
	})
}
