tag-patch:
	@latest_tag=$$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$$' | head -n 1); \
	echo "Latest tag: $$latest_tag"; \
	if [ -z "$$latest_tag" ]; then \
		new_tag="v0.0.1"; \
	else \
		major=$$(echo $$latest_tag | cut -d. -f1 | tr -d 'v'); \
		minor=$$(echo $$latest_tag | cut -d. -f2); \
		patch=$$(echo $$latest_tag | cut -d. -f3); \
		new_patch=$$(($$patch + 1)); \
		new_tag="v$$major.$$minor.$$new_patch"; \
	fi; \
	echo "Creating tag $$new_tag"; \
	git tag $$new_tag; \
	git push origin $$new_tag
