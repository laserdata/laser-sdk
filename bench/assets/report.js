const themeToggle = document.getElementById('theme-toggle')

function renderThemeToggle() {
    const theme = document.documentElement.dataset.theme
    themeToggle.textContent = `Theme: ${theme}`
    themeToggle.setAttribute('aria-label', `Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`)
}

themeToggle.addEventListener('click', () => {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark'
    document.documentElement.dataset.theme = next
    try { window.localStorage.setItem('laser-bench-theme', next) } catch (_) {}
    renderThemeToggle()
})

renderThemeToggle()
