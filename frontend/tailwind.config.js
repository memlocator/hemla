/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,svelte}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Space Grotesk', 'Avenir Next', 'sans-serif'],
        brand: ['Fraunces', 'Georgia', 'serif']
      },
      colors: {
        ink: '#0e1e2b',
        calm: '#0f766e'
      },
      boxShadow: {
        panel: '0 18px 42px -24px rgba(17, 52, 80, 0.45)'
      },
      backgroundImage: {
        canvas: 'radial-gradient(circle at 15% 20%, #f5faf9 0%, #ecf3fa 40%, #dfebf8 100%)'
      }
    }
  },
  plugins: []
};
