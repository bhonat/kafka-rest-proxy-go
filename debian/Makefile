VERSION := $(shell cat ../VERSION)

.PHONY: package
package:
	dpkg-buildpackage -us -uc
