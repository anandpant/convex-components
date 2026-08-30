import convexPlugin from "@convex-dev/eslint-plugin";
import pluginJs from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

export default [
  {
    ignores: ["**/dist/**", "**/_generated/**", "**/test-fixtures/**"],
  },
  pluginJs.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["packages/**/*.ts"],
    languageOptions: {
      globals: globals.worker,
      parser: tseslint.parser,
      parserOptions: {
        project: [
          "./packages/*/tsconfig.json",
          "./packages/*/tsconfig.test.json",
          "./packages/*/example/tsconfig.json",
        ],
        tsconfigRootDir: import.meta.dirname,
      },
    },
    plugins: {
      "@convex-dev": convexPlugin,
    },
    rules: {
      ...convexPlugin.configs.recommended[0].rules,
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      "no-unused-vars": "off",
    },
  },
  {
    files: ["packages/**/*.test.ts", "packages/**/test.ts"],
    languageOptions: {
      globals: { ...globals.node, ...globals.worker },
    },
  },
  {
    files: ["scripts/*.mjs", "packages/**/scripts/*.mjs"],
    languageOptions: { globals: globals.node },
  },
];
