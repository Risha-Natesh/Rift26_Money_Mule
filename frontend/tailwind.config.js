/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx,ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        display: ["Space Grotesk", "Segoe UI", "sans-serif"],
        body: ["IBM Plex Sans", "Segoe UI", "sans-serif"]
      },
      colors: {
        risk: {
          high: "#ff4d4f",
          medium: "#f59e0b",
          safe: "#94a3b8"
        }
      }
    }
  },
  plugins: []
};
