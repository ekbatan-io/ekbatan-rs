# The ekbatan-rs website

An Astro static site, deployed as this repository's GitHub Page by
`.github/workflows/deploy-website.yml`.

```bash
cd website
npm ci
npm run dev       # http://localhost:4321
npm run build     # -> dist/
BASE=/ekbatan-rs/ npm run build     # what CI builds
npm run check     # after a build: unknown CSS classes, broken internal links
```

Node is the only part of this repository outside Cargo, and it stays in this folder and its
own workflow.

## Sections

- **`/learn/`**: a guided path, read in order. The list is `src/lib/learn.ts`; each page is
  `src/pages/learn/<slug>.mdx`.
- **`/reference/`**: one page per class or part of the framework, for looking things up. The
  list is `src/pages/reference/index.astro`.
- **`/concepts/`**: short links into the Learn pages that explain an idea.

Every Learn and Reference page has a twin in the repository's `docs/` folder at the same path
(`docs/learn/<slug>.md`, `docs/reference/<path>.md`), written at the same time, so the
repository can be read without the site.

## Copied, not rewritten

The design (styles, fonts, layouts, components) is the Java and Python sites', so the three are
recognisably one project. `src/styles/styles.css` is a compiled Tailwind bundle, shipped as it
is: a utility class it doesn't define does nothing, silently, which is what
`scripts/check-classes.mjs` catches. `src/styles/palette.css` is the one place colours can
differ (see `PALETTES.md`).
