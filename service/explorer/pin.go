package explorer

import (
	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	usersvc "github.com/cloudreve/Cloudreve/v4/service/user"
	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
)

type (
	PinFileService struct {
		Uri  string `json:"uri" binding:"required"`
		Name string `json:"name"`
	}
	PinFileParameterCtx struct{}
)

// PinFileService pins new uri to sidebar
func (service *PinFileService) PinFile(c *gin.Context) error {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	userClient := dep.UserClient()

	uri, err := fs.NewUriFromString(service.Uri)
	if err != nil {
		return serializer.NewError(serializer.CodeParamErr, "unknown uri", err)
	}

	uriStr := uri.String()
	// 已由用户组默认固定的分享无需重复固定
	if usersvc.IsGroupDefaultPin(uriStr, usersvc.BuildGroupDefaultPins(user.Edges.Group, dep.HashIDEncoder())) {
		return nil
	}

	for _, pin := range user.Settings.Pined {
		if pin.Uri == uriStr {
			if pin.Name != service.Name {
				return serializer.NewError(serializer.CodeObjectExist, "uri already pinned with different name", nil)
			}

			return nil
		}
	}

	user.Settings.Pined = append(user.Settings.Pined, types.PinedFile{
		Uri:  uriStr,
		Name: service.Name,
	})
	if err := userClient.SaveSettings(c, user); err != nil {
		return serializer.NewError(serializer.CodeDBError, "failed to save settings", err)
	}

	return nil
}

// UnpinFile removes uri from sidebar
func (service *PinFileService) UnpinFile(c *gin.Context) error {
	dep := dependency.FromContext(c)
	user := inventory.UserFromContext(c)
	userClient := dep.UserClient()

	uri, err := fs.NewUriFromString(service.Uri)
	if err != nil {
		return serializer.NewError(serializer.CodeParamErr, "unknown uri", err)
	}

	uriStr := uri.String()
	// 用户组默认固定的分享不允许用户取消固定
	if usersvc.IsGroupDefaultPin(uriStr, usersvc.BuildGroupDefaultPins(user.Edges.Group, dep.HashIDEncoder())) {
		return serializer.NewError(serializer.CodeNoPermissionErr, "Cannot unpin group default pinned share", nil)
	}

	user.Settings.Pined = lo.Filter(user.Settings.Pined, func(pin types.PinedFile, index int) bool {
		return pin.Uri != uriStr
	})

	if err := userClient.SaveSettings(c, user); err != nil {
		return serializer.NewError(serializer.CodeDBError, "failed to save settings", err)
	}

	return nil
}
