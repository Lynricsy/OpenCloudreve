package controllers

import (
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/cloudreve/Cloudreve/v4/service/admin"
	"github.com/gin-gonic/gin"
)

// respond 统一输出 Google Drive 备份接口的结果。
func respond(c *gin.Context, data any, err error) {
	if err != nil {
		c.JSON(200, serializer.Err(c, err))
		return
	}
	c.JSON(200, serializer.Response{Data: data})
}

func AdminBackupStatus(c *gin.Context) {
	res, err := admin.GetBackupStatus(c)
	respond(c, res, err)
}

func AdminBackupOAuthURL(c *gin.Context) {
	res, err := admin.GetBackupOAuthURL(c)
	respond(c, res, err)
}

func AdminFinishBackupOAuth(c *gin.Context) {
	service := ParametersFromContext[*admin.FinishBackupOAuthService](c, admin.FinishBackupOAuthParamCtx{})
	respond(c, nil, service.Finish(c))
}

func AdminDisconnectBackup(c *gin.Context) {
	respond(c, nil, admin.DisconnectBackup(c))
}

func AdminGenerateBackupKey(c *gin.Context) {
	res, err := admin.GenerateBackupKey()
	respond(c, res, err)
}

func AdminRunBackup(c *gin.Context) {
	respond(c, nil, admin.RunBackup(c))
}

func AdminListBackupSnapshots(c *gin.Context) {
	res, err := admin.ListBackupSnapshots(c)
	respond(c, res, err)
}

func AdminBrowseBackup(c *gin.Context) {
	service := ParametersFromContext[*admin.BrowseBackupService](c, admin.BrowseBackupParamCtx{})
	res, err := service.Browse(c)
	respond(c, res, err)
}

func AdminRestoreBackup(c *gin.Context) {
	service := ParametersFromContext[*admin.RestoreBackupService](c, admin.RestoreBackupParamCtx{})
	respond(c, nil, service.Restore(c))
}
