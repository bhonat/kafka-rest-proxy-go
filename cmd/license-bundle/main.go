package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type moduleInfo struct {
	Path    string
	Version string
	Dir     string
	Main    bool
}

type moduleNotice struct {
	Path        string
	Version     string
	LicenseRefs []string
	SHA256      string
	Missing     bool
}

func main() {
	outDir := flag.String("out", "licenses", "output directory for generated third-party license bundle")
	flag.Parse()

	if err := run(*outDir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(outDir string) error {
	modules, err := listModules()
	if err != nil {
		return err
	}
	notices, err := writeLicenseBundle(outDir, modules)
	if err != nil {
		return err
	}
	fmt.Printf("generated %s with %d third-party modules\n", filepath.Join(outDir, "THIRD_PARTY_NOTICES.md"), len(notices))
	return nil
}

func listModules() ([]moduleInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	download := exec.CommandContext(ctx, "go", "mod", "download", "all")
	if out, err := download.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("go mod download all: %w\n%s", err, string(out))
	}

	cmd := exec.CommandContext(ctx, "go", "list", "-m", "-json", "all")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("go list modules: %w\n%s", err, string(out))
	}

	dec := json.NewDecoder(bytes.NewReader(out))
	var modules []moduleInfo
	for dec.More() {
		var mod moduleInfo
		if err := dec.Decode(&mod); err != nil {
			return nil, fmt.Errorf("decode module list: %w", err)
		}
		if mod.Main {
			continue
		}
		modules = append(modules, mod)
	}
	sort.Slice(modules, func(i, j int) bool {
		if modules[i].Path == modules[j].Path {
			return modules[i].Version < modules[j].Version
		}
		return modules[i].Path < modules[j].Path
	})
	return modules, nil
}

func writeLicenseBundle(outDir string, modules []moduleInfo) ([]moduleNotice, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	modulesDir := filepath.Join(outDir, "modules")
	if err := os.RemoveAll(modulesDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(modulesDir, 0o755); err != nil {
		return nil, err
	}

	var notices []moduleNotice
	for _, mod := range modules {
		notice, err := copyModuleLicenses(modulesDir, mod)
		if err != nil {
			return nil, err
		}
		notices = append(notices, notice)
	}

	if err := os.WriteFile(filepath.Join(modulesDir, "README.md"), []byte(modulesReadme()), 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(outDir, "THIRD_PARTY_NOTICES.md"), []byte(noticesMarkdown(notices)), 0o644); err != nil {
		return nil, err
	}
	return notices, nil
}

func copyModuleLicenses(modulesDir string, mod moduleInfo) (moduleNotice, error) {
	notice := moduleNotice{
		Path:    mod.Path,
		Version: mod.Version,
	}

	if mod.Dir == "" {
		notice.Missing = true
		return notice, nil
	}

	entries, err := os.ReadDir(mod.Dir)
	if err != nil {
		notice.Missing = true
		return notice, nil
	}

	moduleOutDir := filepath.Join(modulesDir, safeModuleDir(mod))
	for _, entry := range entries {
		if entry.IsDir() || !isLicenseLike(entry) {
			continue
		}
		src := filepath.Join(mod.Dir, entry.Name())
		body, err := os.ReadFile(src)
		if err != nil {
			return notice, fmt.Errorf("read license file %s: %w", src, err)
		}
		if err := os.MkdirAll(moduleOutDir, 0o755); err != nil {
			return notice, err
		}
		dstName := sanitizeFileName(entry.Name())
		dst := filepath.Join(moduleOutDir, dstName)
		if err := os.WriteFile(dst, body, 0o644); err != nil {
			return notice, fmt.Errorf("write license file %s: %w", dst, err)
		}
		sum := sha256.Sum256(body)
		notice.SHA256 = hex.EncodeToString(sum[:])
		notice.LicenseRefs = append(notice.LicenseRefs, filepath.ToSlash(filepath.Join("modules", safeModuleDir(mod), dstName)))
	}

	if len(notice.LicenseRefs) == 0 {
		notice.Missing = true
	}
	sort.Strings(notice.LicenseRefs)
	return notice, nil
}

func isLicenseLike(entry fs.DirEntry) bool {
	name := strings.ToLower(entry.Name())
	name = strings.TrimSuffix(name, ".md")
	name = strings.TrimSuffix(name, ".txt")
	name = strings.TrimSuffix(name, ".rst")
	return name == "license" ||
		name == "licence" ||
		name == "copying" ||
		name == "notice" ||
		name == "copyright" ||
		strings.HasPrefix(name, "license-") ||
		strings.HasPrefix(name, "license.") ||
		strings.HasPrefix(name, "licence-") ||
		strings.HasPrefix(name, "licence.")
}

func safeModuleDir(mod moduleInfo) string {
	version := mod.Version
	if version == "" {
		version = "unknown"
	}
	raw := mod.Path + "@" + version
	replacer := strings.NewReplacer("/", "__", "\\", "__", ":", "_", " ", "_", "@", "_at_")
	return replacer.Replace(raw)
}

func sanitizeFileName(name string) string {
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, string(filepath.Separator), "_")
	if name == "." || name == "" {
		return "LICENSE"
	}
	return name
}

func noticesMarkdown(notices []moduleNotice) string {
	var b strings.Builder
	b.WriteString("# Third-party notices\n\n")
	b.WriteString("This file is generated from `go list -m -json all` by `make generate-licenses`.\n")
	b.WriteString("Do not edit generated module entries by hand.\n\n")
	b.WriteString("| Module | Version | License files | Review |\n")
	b.WriteString("|---|---:|---|---|\n")
	for _, notice := range notices {
		refs := "REVIEW_NEEDED"
		review := ""
		if len(notice.LicenseRefs) > 0 {
			links := make([]string, 0, len(notice.LicenseRefs))
			for _, ref := range notice.LicenseRefs {
				links = append(links, fmt.Sprintf("[%s](%s)", filepath.Base(ref), ref))
			}
			refs = strings.Join(links, ", ")
		}
		if notice.Missing {
			review = "missing license file in module cache"
		}
		b.WriteString(fmt.Sprintf("| `%s` | `%s` | %s | %s |\n", notice.Path, notice.Version, refs, review))
	}
	b.WriteString("\n")
	b.WriteString("Release reviewers should confirm this generated bundle before public distribution.\n")
	return b.String()
}

func modulesReadme() string {
	return "# Generated module license files\n\n" +
		"This directory is generated by `make generate-licenses`.\n" +
		"It contains copied license/notice files from Go module cache directories.\n"
}
