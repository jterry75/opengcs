package gcs

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/Microsoft/opengcs/service/gcs/oslayer/realos"
	"github.com/Microsoft/opengcs/service/gcs/prot"
	"github.com/Microsoft/opengcs/service/gcs/runtime/runc"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Storage", func() {
	var (
		coreint *gcsCore
	)

	BeforeEach(func() {
		rtime, err := runc.NewRuntime()
		Expect(err).NotTo(HaveOccurred())
		os := realos.NewOS()
		coreint = NewGCSCore(rtime, os)
	})

	Describe("getting the container paths", func() {
		var (
			validID string
		)
		BeforeEach(func() {
			validID = "abcdef-ghi"
		})

		Describe("getting the container storage path", func() {
			Context("when the ID is a valid string", func() {
				It("should return the correct path", func() {
					Expect(coreint.getContainerStoragePath(validID)).To(Equal("/tmp/gcs/abcdef-ghi"))
				})
			})
		})

		Describe("getting the unioning paths", func() {
			Context("when the ID is a valid string", func() {
				It("should return the correct paths", func() {
					layerPrefix, scratchPath, workdirPath, rootfsPath := coreint.getUnioningPaths(validID)
					Expect(layerPrefix).To(Equal("/tmp/gcs/abcdef-ghi/layer"))
					Expect(scratchPath).To(Equal("/tmp/gcs/abcdef-ghi/scratch"))
					Expect(workdirPath).To(Equal("/tmp/gcs/abcdef-ghi/scratch/work"))
					Expect(rootfsPath).To(Equal("/tmp/gcs/abcdef-ghi/rootfs"))
				})
			})
		})

		Describe("getting the config path", func() {
			Context("when the ID is a valid string", func() {
				It("should return the correct path", func() {
					Expect(coreint.getConfigPath(validID)).To(Equal("/tmp/gcs/abcdef-ghi/config.json"))
				})
			})
		})
	})

	// TODO: This test and the PathIsMounted test should be moved to a new
	// testing suite for realos.
	Describe("checking if a path exists", func() {
		var (
			dirToTest  string
			fileToTest string
			path       string
			exists     bool
			err        error
		)
		BeforeEach(func() {
			dirToTest = "/tmp/testdir"
			fileToTest = "/tmp/testfile"
		})
		JustBeforeEach(func() {
			exists, err = coreint.OS.PathExists(path)
		})
		AssertDoesNotExist := func() {
			It("should not report exists", func() {
				Expect(exists).To(BeFalse())
			})
			It("should not return an error", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		}
		AssertExists := func() {
			It("should report exists", func() {
				Expect(exists).To(BeTrue())
			})
			It("should not return an error", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		}
		Context("the paths don't exist", func() {
			Context("using the directory path", func() {
				BeforeEach(func() {
					path = dirToTest
				})
				AssertDoesNotExist()
			})
			Context("using the file path", func() {
				BeforeEach(func() {
					path = fileToTest
				})
				AssertDoesNotExist()
			})
		})
		Context("the paths exist", func() {
			BeforeEach(func() {
				err := os.Mkdir(dirToTest, 0600)
				Expect(err).NotTo(HaveOccurred())
				_, err = os.OpenFile(fileToTest, os.O_RDONLY|os.O_CREATE, 0600)
				Expect(err).NotTo(HaveOccurred())
			})
			AfterEach(func() {
				err := os.Remove(dirToTest)
				Expect(err).NotTo(HaveOccurred())
				err = os.Remove(fileToTest)
				Expect(err).NotTo(HaveOccurred())
			})
			Context("using the directory path", func() {
				BeforeEach(func() {
					path = dirToTest
				})
				AssertExists()
			})
			Context("using the file path", func() {
				BeforeEach(func() {
					path = fileToTest
				})
				AssertExists()
			})
		})
	})

	// TODO: This test and the PathExists test should be moved to a new testing
	// suite for realos.
	Describe("checking if a path is mounted", func() {
		var (
			mountSource string
			mountTarget string
			mounted     bool
			err         error
		)
		BeforeEach(func() {
			mountSource = "/tmp/mountsource"
			mountTarget = "/tmp/mounttarget"
			err := os.Mkdir(mountSource, 0600)
			Expect(err).NotTo(HaveOccurred())
			err = os.Mkdir(mountTarget, 0600)
			Expect(err).NotTo(HaveOccurred())
		})
		AfterEach(func() {
			err := os.Remove(mountSource)
			Expect(err).NotTo(HaveOccurred())
			err = os.Remove(mountTarget)
			Expect(err).NotTo(HaveOccurred())
		})
		JustBeforeEach(func() {
			mounted, err = coreint.OS.PathIsMounted(mountTarget)
		})
		Context("the source isn't mounted", func() {
			It("should not report mounted", func() {
				Expect(mounted).To(BeFalse())
			})
			It("should not return an error", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})
		Context("the source is mounted", func() {
			BeforeEach(func() {
				err := syscall.Mount(mountSource, mountTarget, defaultFileSystem, syscall.MS_BIND, "")
				Expect(err).NotTo(HaveOccurred())
			})
			AfterEach(func() {
				syscall.Unmount(mountTarget, 0)
				Expect(err).NotTo(HaveOccurred())
			})
			It("should report mounted", func() {
				Expect(mounted).To(BeTrue())
			})
			It("should not return an error", func() {
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Describe("mounting and unmounting layers", func() {
		var (
			containerID string
			err         error
		)
		BeforeEach(func() {
			containerID = "abcdef-ghi"
		})
		SetupLoopbacks := func(layers []string) {
			for i, layer := range layers {
				out, err := exec.Command("losetup", fmt.Sprintf("/dev/loop%d", i), layer).CombinedOutput()
				if err != nil {
					// Provide some extra information to the error.
					err = fmt.Errorf("%s: %s", out, err)
					Expect(err).NotTo(HaveOccurred())
				}
			}
		}
		UnsetupLoopbacks := func(numLoopbacks int) {
			for i := 0; i < numLoopbacks; i++ {
				path := fmt.Sprintf("/dev/loop%d", i)
				out, err := exec.Command("losetup", "-d", path).CombinedOutput()
				if err != nil {
					// Provide some extra information to the error.
					err = fmt.Errorf("%s: %s", out, err)
					Expect(err).NotTo(HaveOccurred())
				}

				mounted, err := coreint.OS.PathIsMounted(path)
				Expect(err).NotTo(HaveOccurred())
				if mounted {
					out, err = exec.Command("umount", path).CombinedOutput()
					if err != nil {
						err = fmt.Errorf("%s: %s", out, err)
						Expect(err).NotTo(HaveOccurred())
					}
				}
			}
		}
		GenerateLayers := func(layers []string, fileMaps []map[string]string) {
			if fileMaps != nil {
				Expect(layers).To(HaveLen(len(fileMaps)))
			}
			for i, layer := range layers {
				// Create the layer file.
				out, err := exec.Command("dd", "if=/dev/zero", fmt.Sprintf("of=%s", layer), "bs=1M", "count=16").CombinedOutput()
				if err != nil {
					// Provide some extra information to the error.
					err = fmt.Errorf("%s: %s", out, err)
					Expect(err).NotTo(HaveOccurred())
				}

				// Give it an ext4 filesystem.
				out, err = exec.Command("mkfs.ext4", "-F", layer).CombinedOutput()
				if err != nil {
					// Provide some extra information to the error.
					err = fmt.Errorf("%s: %s", out, err)
					Expect(err).NotTo(HaveOccurred())
				}

				if fileMaps != nil {
					// Mount the new layer to a directory.
					tempDir, err := ioutil.TempDir("", "gcs_test_layer")
					Expect(err).NotTo(HaveOccurred())
					out, err = exec.Command("mount", "-o", "sync,dirsync", layer, tempDir).CombinedOutput()
					if err != nil {
						// Provide some extra information to the error.
						err = fmt.Errorf("%s: %s", out, err)
						Expect(err).NotTo(HaveOccurred())
					}

					// Create files in the layer.
					for file, contents := range fileMaps[i] {
						err := ioutil.WriteFile(filepath.Join(tempDir, file), []byte(contents), 0777)
						Expect(err).NotTo(HaveOccurred())
					}

					// Unmount the layer.
					err = syscall.Unmount(tempDir, 0)
					Expect(err).NotTo(HaveOccurred())
				}
			}
		}
		DestroyLayers := func(layers []string) {
			for _, layer := range layers {
				err := os.Remove(layer)
				Expect(err).NotTo(HaveOccurred())
			}
		}
		CheckFileContents := func(path, name string, expectedContents string) {
			By(fmt.Sprintf("checking file %s", name))
			actualContents, err := ioutil.ReadFile(filepath.Join(path, name))
			Expect(err).NotTo(HaveOccurred())
			Expect(string(actualContents)).To(Equal(expectedContents))
		}
		Context("using three basic layers", func() {
			var (
				layers      []string
				scratchSpec *mountSpec
				layerSpecs  []*mountSpec
			)
			BeforeEach(func() {
				// This test's file contents are as follows:
				//
				// layer1:        file2         file4         file6
				// layer2: file1                file4  file5
				// layer3: file1  file2  file3
				//
				// Each file in each layer contains only the name of the layer it is in. For
				// example, each file in layer1 contains only the text "layer1". This is useful
				// for determining each file's originating layer in a union filesystem.
				layer1Files := map[string]string{
					"file2": "layer1",
					"file4": "layer1",
					"file6": "layer1",
				}
				layer2Files := map[string]string{
					"file1": "layer2",
					"file4": "layer2",
					"file5": "layer2",
				}
				layer3Files := map[string]string{
					"file1": "layer3",
					"file2": "layer3",
					"file3": "layer3",
				}
				layers = []string{"scratch", "layer1", "layer2", "layer3"}
				files := []map[string]string{map[string]string{}, layer1Files, layer2Files, layer3Files}
				GenerateLayers(layers, files)
				SetupLoopbacks(layers)
				scratchSpec = &mountSpec{
					Source:     "/dev/loop0",
					FileSystem: defaultFileSystem,
				}
				layerSpecs = []*mountSpec{
					{Source: "/dev/loop1", FileSystem: defaultFileSystem, Flags: syscall.MS_RDONLY, Options: []string{mountOptionNoLoad}},
					{Source: "/dev/loop2", FileSystem: defaultFileSystem, Flags: syscall.MS_RDONLY, Options: []string{mountOptionNoLoad}},
					{Source: "/dev/loop3", FileSystem: defaultFileSystem, Flags: syscall.MS_RDONLY, Options: []string{mountOptionNoLoad}},
				}
			})
			AfterEach(func() {
				UnsetupLoopbacks(4)
				// Make sure to clean up in case the test fails halfway
				// through.
				coreint.unmountLayers(containerID)
				coreint.destroyContainerStorage(containerID)
				DestroyLayers(layers)
			})
			It("should behave properly", func() {
				// Mount the layers.
				err = coreint.mountLayers(containerID, scratchSpec, layerSpecs)
				Expect(err).NotTo(HaveOccurred())

				containerPath := filepath.Join("/tmp", "gcs", containerID)

				// Check the state of rootfs.
				rootfsPath := filepath.Join(containerPath, "rootfs")
				exists, err := coreint.OS.PathExists(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err := coreint.OS.PathIsMounted(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of scratch.
				scratchPath := filepath.Join(containerPath, "scratch")
				exists, err = coreint.OS.PathExists(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of layer0.
				layer0Path := filepath.Join(containerPath, "layer0")
				exists, err = coreint.OS.PathExists(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of layer1.
				layer1Path := filepath.Join(containerPath, "layer1")
				exists, err = coreint.OS.PathExists(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of layer2.
				layer2Path := filepath.Join(containerPath, "layer2")
				exists, err = coreint.OS.PathExists(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check that layers were mounted in the correct order.
				CheckFileContents(rootfsPath, "file1", "layer2")
				CheckFileContents(rootfsPath, "file2", "layer1")
				CheckFileContents(rootfsPath, "file3", "layer3")
				CheckFileContents(rootfsPath, "file4", "layer1")
				CheckFileContents(rootfsPath, "file5", "layer2")
				CheckFileContents(rootfsPath, "file6", "layer1")

				// Unmount the layers.
				err = coreint.unmountLayers(containerID)
				Expect(err).NotTo(HaveOccurred())

				// Check the final state of the layers.
				mounted, err = coreint.OS.PathIsMounted(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Detroy the layers.
				err = coreint.destroyContainerStorage(containerID)
				Expect(err).NotTo(HaveOccurred())
				exists, err = coreint.OS.PathExists(containerPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})
		})
		Context("with no scratch device", func() {
			var (
				layers     []string
				layerSpecs []*mountSpec
			)
			BeforeEach(func() {
				layers = []string{"layer1", "layer2", "layer3"}
				GenerateLayers(layers, nil)
				SetupLoopbacks(layers)
				layerSpecs = []*mountSpec{
					{Source: "/dev/loop0", FileSystem: defaultFileSystem, Flags: syscall.MS_RDONLY, Options: []string{mountOptionNoLoad}},
					{Source: "/dev/loop1", FileSystem: defaultFileSystem, Flags: syscall.MS_RDONLY, Options: []string{mountOptionNoLoad}},
					{Source: "/dev/loop2", FileSystem: defaultFileSystem, Flags: syscall.MS_RDONLY, Options: []string{mountOptionNoLoad}},
				}
			})
			AfterEach(func() {
				UnsetupLoopbacks(3)
				DestroyLayers(layers)
			})
			It("should behave properly", func() {
				// Mount the layers.
				err = coreint.mountLayers(containerID, nil, layerSpecs)
				Expect(err).NotTo(HaveOccurred())

				containerPath := filepath.Join("/tmp", "gcs", containerID)

				// Check the state of rootfs.
				rootfsPath := filepath.Join(containerPath, "rootfs")
				exists, err := coreint.OS.PathExists(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err := coreint.OS.PathIsMounted(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of scratch.
				scratchPath := filepath.Join(containerPath, "scratch")
				exists, err = coreint.OS.PathExists(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Check the state of layer0.
				layer0Path := filepath.Join(containerPath, "layer0")
				exists, err = coreint.OS.PathExists(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of layer1.
				layer1Path := filepath.Join(containerPath, "layer1")
				exists, err = coreint.OS.PathExists(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of layer2.
				layer2Path := filepath.Join(containerPath, "layer2")
				exists, err = coreint.OS.PathExists(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Unmount the layers.
				err = coreint.unmountLayers(containerID)
				Expect(err).NotTo(HaveOccurred())

				// Check the final state of the layers.
				mounted, err = coreint.OS.PathIsMounted(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Detroy the layers.
				err = coreint.destroyContainerStorage(containerID)
				Expect(err).NotTo(HaveOccurred())
				exists, err = coreint.OS.PathExists(containerPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})
		})
		Context("with no layers", func() {
			var (
				layers      []string
				scratchSpec *mountSpec
			)
			BeforeEach(func() {
				layers = []string{"scratch"}
				GenerateLayers(layers, nil)
				SetupLoopbacks(layers)
				scratchSpec = &mountSpec{
					Source:     "/dev/loop0",
					FileSystem: defaultFileSystem,
				}
			})
			AfterEach(func() {
				UnsetupLoopbacks(1)
				DestroyLayers(layers)
			})
			It("should behave properly", func() {
				// Mount the layers.
				err = coreint.mountLayers(containerID, scratchSpec, nil)
				Expect(err).NotTo(HaveOccurred())

				containerPath := filepath.Join("/tmp", "gcs", containerID)

				// Check the state of rootfs.
				rootfsPath := filepath.Join(containerPath, "rootfs")
				exists, err := coreint.OS.PathExists(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err := coreint.OS.PathIsMounted(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of scratch.
				scratchPath := filepath.Join(containerPath, "scratch")
				exists, err = coreint.OS.PathExists(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of layer0.
				layer0Path := filepath.Join(containerPath, "layer0")
				exists, err = coreint.OS.PathExists(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Check the state of layer1.
				layer1Path := filepath.Join(containerPath, "layer1")
				exists, err = coreint.OS.PathExists(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Check the state of layer2.
				layer2Path := filepath.Join(containerPath, "layer2")
				exists, err = coreint.OS.PathExists(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Unmount the layers.
				err = coreint.unmountLayers(containerID)
				Expect(err).NotTo(HaveOccurred())

				// Check the final state of the layers.
				mounted, err = coreint.OS.PathIsMounted(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Detroy the layers.
				err = coreint.destroyContainerStorage(containerID)
				Expect(err).NotTo(HaveOccurred())
				exists, err = coreint.OS.PathExists(containerPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})
		})
		Context("with no scratch device or layers", func() {
			var (
				layers []string
			)
			BeforeEach(func() {
				layers = []string{}
				GenerateLayers(layers, nil)
				SetupLoopbacks(layers)
			})
			AfterEach(func() {
				UnsetupLoopbacks(0)
				DestroyLayers(layers)
			})
			It("should behave properly", func() {
				// Mount the layers.
				err = coreint.mountLayers(containerID, nil, nil)
				Expect(err).NotTo(HaveOccurred())

				containerPath := filepath.Join("/tmp", "gcs", containerID)

				// Check the state of rootfs.
				rootfsPath := filepath.Join(containerPath, "rootfs")
				exists, err := coreint.OS.PathExists(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err := coreint.OS.PathIsMounted(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeTrue())

				// Check the state of scratch.
				scratchPath := filepath.Join(containerPath, "scratch")
				exists, err = coreint.OS.PathExists(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeTrue())
				mounted, err = coreint.OS.PathIsMounted(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Check the state of layer0.
				layer0Path := filepath.Join(containerPath, "layer0")
				exists, err = coreint.OS.PathExists(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Check the state of layer1.
				layer1Path := filepath.Join(containerPath, "layer1")
				exists, err = coreint.OS.PathExists(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Check the state of layer2.
				layer2Path := filepath.Join(containerPath, "layer2")
				exists, err = coreint.OS.PathExists(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Unmount the layers.
				err = coreint.unmountLayers(containerID)
				Expect(err).NotTo(HaveOccurred())

				// Check the final state of the layers.
				mounted, err = coreint.OS.PathIsMounted(rootfsPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(scratchPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer0Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer1Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())
				mounted, err = coreint.OS.PathIsMounted(layer2Path)
				Expect(err).NotTo(HaveOccurred())
				Expect(mounted).To(BeFalse())

				// Detroy the layers.
				err = coreint.destroyContainerStorage(containerID)
				Expect(err).NotTo(HaveOccurred())
				exists, err = coreint.OS.PathExists(containerPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(exists).To(BeFalse())
			})
		})
		Describe("mounting and unmounting mapped virtual disks", func() {
			Context("mounting two basic layers", func() {
				var (
					layers     []string
					layer1Path string
					layer2Path string
					disk1      prot.MappedVirtualDisk
					disk2      prot.MappedVirtualDisk
				)
				BeforeEach(func() {
					layers = []string{"layer1", "layer2"}
					GenerateLayers(layers, nil)
					SetupLoopbacks(layers)
					coreint.containerCache[containerID] = newContainerCacheEntry(containerID)
					layer1Path = "/mnt/test/layer1"
					layer2Path = "/mnt/test/layer2"
					disk1 = prot.MappedVirtualDisk{
						ContainerPath:     layer1Path,
						Lun:               0,
						CreateInUtilityVM: true,
						ReadOnly:          true,
					}
					disk2 = prot.MappedVirtualDisk{
						ContainerPath:     layer2Path,
						Lun:               1,
						CreateInUtilityVM: true,
						ReadOnly:          false,
					}
				})
				AfterEach(func() {
					UnsetupLoopbacks(2)
					// Make sure to clean up in case the test fails halfway
					// through.
					err = coreint.unmountMappedVirtualDisks([]prot.MappedVirtualDisk{disk1, disk2})
					Expect(err).NotTo(HaveOccurred())
					err = os.RemoveAll(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					err = os.RemoveAll(layer2Path)
					Expect(err).NotTo(HaveOccurred())
					DestroyLayers(layers)
				})
				It("should behave properly", func() {
					// Mount the disks.
					err = coreint.containerCache[containerID].AddMappedVirtualDisk(disk1)
					Expect(err).NotTo(HaveOccurred())
					err = coreint.containerCache[containerID].AddMappedVirtualDisk(disk2)
					Expect(err).NotTo(HaveOccurred())
					ms := []*mountSpec{
						{Source: "/dev/loop0", FileSystem: defaultFileSystem, Flags: syscall.MS_RDONLY},
						{Source: "/dev/loop1", FileSystem: defaultFileSystem},
					}
					err = coreint.mountMappedVirtualDisks([]prot.MappedVirtualDisk{disk1, disk2}, ms)
					Expect(err).NotTo(HaveOccurred())

					// Check the state of layer1.
					exists, err := coreint.OS.PathExists(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue())
					mounted, err := coreint.OS.PathIsMounted(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(mounted).To(BeTrue())
					// TODO: Check if readonly.

					// Check the state of layer2.
					exists, err = coreint.OS.PathExists(layer2Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue())
					mounted, err = coreint.OS.PathIsMounted(layer2Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(mounted).To(BeTrue())
					// TODO: Check if readonly.

					// Unmount the disks.
					err = coreint.unmountMappedVirtualDisks([]prot.MappedVirtualDisk{disk1, disk2})
					Expect(err).NotTo(HaveOccurred())

					// Check the final state of layer1.
					exists, err = coreint.OS.PathExists(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue())
					mounted, err = coreint.OS.PathIsMounted(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(mounted).To(BeFalse())

					// Check the final state of layer2.
					exists, err = coreint.OS.PathExists(layer2Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeTrue())
					mounted, err = coreint.OS.PathIsMounted(layer2Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(mounted).To(BeFalse())
				})
			})
			Context("mounting layers inside the container namespace", func() {
				var (
					layers     []string
					layer1Path string
					layer2Path string
				)
				BeforeEach(func() {
					layers = []string{"layer1", "layer2"}
					GenerateLayers(layers, nil)
					SetupLoopbacks(layers)
					coreint.containerCache[containerID] = newContainerCacheEntry(containerID)
					layer1Path = "/mnt/test/layer1"
					layer2Path = "/mnt/test/layer2"
				})
				AfterEach(func() {
					UnsetupLoopbacks(2)
					// Make sure to clean up in case the test fails halfway
					// through.
					err = os.RemoveAll(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					err = os.RemoveAll(layer2Path)
					Expect(err).NotTo(HaveOccurred())
					DestroyLayers(layers)
				})
				It("should produce errors", func() {
					// Create the disks.
					disk1 := prot.MappedVirtualDisk{
						ContainerPath:     layer1Path,
						Lun:               0,
						CreateInUtilityVM: false,
						ReadOnly:          true,
					}
					err = coreint.containerCache[containerID].AddMappedVirtualDisk(disk1)
					Expect(err).NotTo(HaveOccurred())
					disk2 := prot.MappedVirtualDisk{
						ContainerPath:     layer2Path,
						Lun:               1,
						CreateInUtilityVM: false,
						ReadOnly:          false,
					}
					err = coreint.containerCache[containerID].AddMappedVirtualDisk(disk2)
					Expect(err).NotTo(HaveOccurred())

					// Mount the disks.
					ms1 := []*mountSpec{{Source: "/dev/loop0", FileSystem: defaultFileSystem, Flags: syscall.MS_RDONLY}}
					ms2 := []*mountSpec{{Source: "/dev/loop1", FileSystem: defaultFileSystem}}
					err = coreint.mountMappedVirtualDisks([]prot.MappedVirtualDisk{disk1}, ms1)
					Expect(err).To(HaveOccurred())
					err = coreint.mountMappedVirtualDisks([]prot.MappedVirtualDisk{disk2}, ms2)
					Expect(err).To(HaveOccurred())
				})
			})
			Context("mounting layers for non-existent device", func() {
				var (
					layerPath string
				)
				BeforeEach(func() {
					coreint.containerCache[containerID] = newContainerCacheEntry(containerID)
					layerPath = "/mnt/test/layer"
				})
				AfterEach(func() {
					// Make sure to clean up in case the test fails halfway
					// through.
					err = os.RemoveAll(layerPath)
					Expect(err).NotTo(HaveOccurred())
				})
				It("should produce errors", func() {
					// Create the disks.
					disk := prot.MappedVirtualDisk{
						ContainerPath:     layerPath,
						Lun:               0,
						CreateInUtilityVM: false,
						ReadOnly:          true,
					}
					err = coreint.containerCache[containerID].AddMappedVirtualDisk(disk)
					Expect(err).NotTo(HaveOccurred())

					// Mount the disks.
					ms := []*mountSpec{{Source: "/dev/fakeloop"}}
					err = coreint.mountMappedVirtualDisks([]prot.MappedVirtualDisk{disk}, ms)
					Expect(err).To(HaveOccurred())
				})
			})
			Context("mounting and unmounting mapped virtual disks with AttachOnly true", func() {
				var (
					layers     []string
					layer1Path string
					disk1      prot.MappedVirtualDisk
				)
				BeforeEach(func() {
					layers = []string{"attachonlylayer1"}
					GenerateLayers(layers, nil)
					coreint.containerCache[containerID] = newContainerCacheEntry(containerID)
					layer1Path = "/mnt/test/attachonlylayer1"
					disk1 = prot.MappedVirtualDisk{
						AttachOnly: true,
					}
				})
				AfterEach(func() {
					DestroyLayers(layers)
				})
				It("Not to have mounted.", func() {
					// Call mount on the disks.
					err = coreint.containerCache[containerID].AddMappedVirtualDisk(disk1)
					Expect(err).NotTo(HaveOccurred())

					ms := []*mountSpec{&mountSpec{}}
					err = coreint.mountMappedVirtualDisks([]prot.MappedVirtualDisk{disk1}, ms)
					Expect(err).NotTo(HaveOccurred())

					// Check the state of attachonlylayer1.
					exists, err := coreint.OS.PathExists(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
					mounted, err := coreint.OS.PathIsMounted(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(mounted).To(BeFalse())

					// Verify calling unmount does nothing.
					err = coreint.unmountMappedVirtualDisks([]prot.MappedVirtualDisk{disk1})
					Expect(err).NotTo(HaveOccurred())

					// Check the final state of attachonlylayer1.
					exists, err = coreint.OS.PathExists(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(exists).To(BeFalse())
					mounted, err = coreint.OS.PathIsMounted(layer1Path)
					Expect(err).NotTo(HaveOccurred())
					Expect(mounted).To(BeFalse())
				})
			})
		})
	})
})
