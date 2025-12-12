const js = require('@eslint/js');
const globals = require('globals');

module.exports = [
  {
    ignores: ['node_modules', 'dist', 'coverage'],
  },
  {
    files: ['**/*.js'],
    languageOptions: {
      ecmaVersion: 2021,
      sourceType: 'script',
      globals: globals.node,
    },
    rules: {
      ...js.configs.recommended.rules,
      'no-unused-vars': ['warn', { args: 'after-used', argsIgnorePattern: '^_' }],
      'no-console': 'off',
    },
  },
];
