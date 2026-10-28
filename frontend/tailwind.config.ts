import type { Config } from "tailwindcss";

export default {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        brand: { 50: "#eef6ff", 100: "#d9eaff", 600: "#1f5fbf", 700: "#184c99", 900: "#0f2d5c" },
      },
    },
  },
  plugins: [],
} satisfies Config;
