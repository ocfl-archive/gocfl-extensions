package ext_NNNN_thumbnail

import (
	"context"

	"emperror.dev/errors"
	"github.com/ocfl-archive/gocfl/v3/pkg/ocfllogger"

	"io/fs"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type ThumbnailMeta struct {
	Ext    string
	Width  uint64
	Height uint64
	Mime   string
}

type function struct {
	thumb   *thumbnail
	command string
	args    []string
	timeout time.Duration
	title   string
	id      string
	pronoms []string
	mime    []*regexp.Regexp
}

func (f *function) Thumbnail(source string, dest string, width uint64, height uint64, logger ocfllogger.OCFLLogger) error {
	if f.thumb == nil {
		return errors.New("thumbnail function not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
	defer cancel()
	args := []string{}
	for _, arg := range f.args {
		arg = strings.ReplaceAll(arg, "{source}", filepath.ToSlash(source))
		arg = strings.ReplaceAll(arg, "{destination}", filepath.ToSlash(dest))
		arg = strings.ReplaceAll(arg, "{background}", f.thumb.Background)
		arg = strings.ReplaceAll(arg, "{width}", strconv.FormatUint(width, 10))
		arg = strings.ReplaceAll(arg, "{height}", strconv.FormatUint(height, 10))
		args = append(args, arg)
	}
	logger.Debug().Msgf("%s %v", f.command, args)
	cmd := exec.CommandContext(ctx, f.command, args...)
	cmd.Dir = filepath.Dir(source)
	return errors.Wrapf(cmd.Run(), "cannot run command '%s %s'", f.command, strings.Join(args, " "))
}

func (f *function) GetID() string {
	return f.id
}

type thumbnail struct {
	Functions  map[string]*function
	SourceFS   fs.FS
	Background string
}

func (m *thumbnail) GetFunctionByName(name string) (*function, error) {
	if f, ok := m.Functions[strings.ToLower(name)]; ok {
		return f, nil
	}
	return nil, errors.Errorf("Thumbnail.Function.%s does not exist", name)
}

func (m *thumbnail) GetFunctionByPronom(pronom string) (*function, error) {
	for _, f := range m.Functions {
		for _, pro := range f.pronoms {
			if pro == pronom {
				return f, nil
			}
		}
	}
	return nil, errors.Errorf("Thumbnail.Source.%s does not exist", pronom)
}

func (m *thumbnail) GetFunctionByMimetype(mime string) (*function, error) {
	for _, f := range m.Functions {
		for _, re := range f.mime {
			if re.MatchString(mime) {
				return f, nil
			}
		}
	}
	return nil, errors.Errorf("Thumbnail.Source.%s does not exist", mime)
}

func (m *thumbnail) SetSourceFS(fs fs.FS) {
	m.SourceFS = fs
}
