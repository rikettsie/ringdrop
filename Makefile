.PHONY: release

# Prepare and push a release.
# Usage: make release VERSION=0.5.0
release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Usage: make release VERSION=x.y.z"; \
		exit 1; \
	fi
	git-cliff -o CHANGELOG.md --tag "v$(VERSION)"
	git add CHANGELOG.md
	git commit -m "chore: release v$(VERSION)"
	git tag "v$(VERSION)"
	git push && git push --tags
