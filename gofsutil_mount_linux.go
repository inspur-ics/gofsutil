package gofsutil

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	procMountsPath = "/proc/self/mountinfo"
	// procMountsRetries is number of times to retry for a consistent
	// read of procMountsPath.
	procMountsRetries = 3
)

var (
	bindRemountOpts = []string{"remount"}
)

// getDiskFormat uses 'blkid' to see if the given disk is unformatted
func (fs *FS) getDiskFormat(ctx context.Context, disk string) (string, error) {

	args := []string{"-s", "TYPE", "-o", "value", disk}

	f := log.Fields{
		"disk": disk,
	}
	log.WithFields(f).WithField("args", args).Info(
		"checking if disk is formatted using blkid")
	buf, err := exec.Command("blkid", args...).CombinedOutput()
	out := string(buf)
	log.WithField("output", out).Debug("blkid output")

	if err != nil {
		log.WithFields(f).WithError(err).Error(
			"no filesystem found on disk")
		return "", nil
	}

	fsType := strings.TrimSpace(out)
	if fsType != "" {
		// The device is formatted
		log.WithFields(f).WithField("fsType", fsType).Info(
			"disk is already formatted")
		return fsType, nil
	}

	// The device is unformatted
	return "", nil
}

// formatAndMount uses unix utils to format and mount the given disk
func (fs *FS) formatAndMount(
	ctx context.Context,
	source, target, fsType string,
	opts ...string) error {

	opts = append(opts, "defaults")
	f := log.Fields{
		"source":  source,
		"target":  target,
		"fsType":  fsType,
		"options": opts,
	}

	existingFormat, err := fs.getDiskFormat(ctx, source)
	if err != nil {
		return err
	}

	if existingFormat == "" {
		// Disk is unformatted so format it.
		args := []string{source}
		// Use 'ext4' as the default
		if len(fsType) == 0 {
			fsType = "ext4"
		}

		if fsType == "ext4" || fsType == "ext3" {
			args = []string{"-F", source}
		}
		f["fsType"] = fsType
		log.WithFields(f).Info(
			"disk appears unformatted, attempting format")

		mkfsCmd := fmt.Sprintf("mkfs.%s", fsType)
		if err := exec.Command(mkfsCmd, args...).Run(); err != nil {
			log.WithFields(f).WithError(err).Error(
				"format of disk failed")
			return err
		}

		// the disk has been formatted successfully try to mount it.
		log.WithFields(f).Info(
			"disk successfully formatted")
		return fs.mount(ctx, source, target, fsType, opts...)
	}

	// Try to mount the disk
	log.WithFields(f).WithField("existingFSType", existingFormat).Info("attempting to mount disk")
	mountErr := fs.mount(ctx, source, target, fsType, opts...)
	if mountErr == nil {
		return nil
	}

	// Disk is already formatted and failed to mount
	if len(fsType) == 0 || fsType == existingFormat {
		// This is mount error
		return mountErr
	}

	// Block device is formatted with unexpected filesystem
	return fmt.Errorf(
		"failed to mount volume as %q; already contains %s: error: %v",
		fsType, existingFormat, mountErr)
}

// bindMount performs a bind mount
func (fs *FS) bindMount(
	ctx context.Context,
	source, target string,
	opts ...string) error {

	err := fs.doMount(ctx, "mount", source, target, "", "bind")
	if err != nil {
		return err
	}
	return fs.doMount(ctx, "mount", source, target, "", opts...)
}

// getMounts returns a slice of all the mounted filesystems
func (fs *FS) getMounts(ctx context.Context) ([]Info, error) {

	_, hash1, err := fs.readProcMounts(ctx, procMountsPath, false)
	if err != nil {
		return nil, err
	}

	for i := 0; i < procMountsRetries; i++ {
		mps, hash2, err := fs.readProcMounts(ctx, procMountsPath, true)
		if err != nil {
			return nil, err
		}
		if hash1 == hash2 {
			// Success
			return mps, nil
		}
		hash1 = hash2
	}
	return nil, fmt.Errorf(
		"failed to get a consistent snapshot of %v after %d tries",
		procMountsPath, procMountsRetries)
}

// readProcMounts reads procMountsInfo and produce a hash
// of the contents and a list of the mounts as Info objects.
func (fs *FS) readProcMounts(
	ctx context.Context,
	path string,
	info bool) ([]Info, uint32, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	return ReadProcMountsFrom(ctx, file, !info, ProcMountsFields, fs.ScanEntry)
}
