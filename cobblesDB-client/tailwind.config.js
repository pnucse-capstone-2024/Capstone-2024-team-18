/** @type {import('tailwindcss').Config} */
const colors = require('tailwindcss/colors')

module.exports = {
  content: ["./src/**/*.{jsx,js}"],
  theme: {
    extend: {
      colors: {
        'matisse': '#355F7D',
      },
    },
    extend: {},
  },
  plugins: [],
}