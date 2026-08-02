import js from "@eslint/js"
import tseslint from "typescript-eslint"
import globals from "globals"

export default tseslint.config(
  js.configs.recommended,
  {
    files: ["**/*.mjs"],
    languageOptions: {
      globals: globals.node
    }
  },
  {
    files: ["**/*.ts"],
    extends: [...tseslint.configs.strictTypeChecked, ...tseslint.configs.stylisticTypeChecked],
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname
      }
    },
    rules: {
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/consistent-type-imports": "error",
      "@typescript-eslint/no-non-null-assertion": "error",
      "@typescript-eslint/switch-exhaustiveness-check": "error",
      "no-restricted-imports": [
        "error",
        {
          paths: [
            {
              name: "apache-iggy",
              message:
                "only src/iggy/apache-iggy.ts may import apache-iggy directly, other src files go through LaserTransport. Tests may use it to exercise the laser.iggyClient escape hatch."
            }
          ]
        }
      ]
    }
  },
  {
    files: ["src/iggy/apache-iggy.ts", "test/**/*.ts"],
    rules: {
      "no-restricted-imports": "off"
    }
  },
  {
    ignores: ["dist/**", "dist-test/**", "node_modules/**", "coverage/**"]
  }
)
