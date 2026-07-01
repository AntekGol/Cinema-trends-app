/**
 * CineTrends - HTMX Event Handlers & UI Enhancements
 */

// Counter animation for KPI cards
function animateCounters() {
    document.querySelectorAll('[data-count]').forEach(el => {
        const target = parseInt(el.dataset.count);
        if (isNaN(target) || target === 0) return;

        const duration = 1200;
        const step = target / (duration / 16);
        let current = 0;

        const timer = setInterval(() => {
            current += step;
            if (current >= target) {
                el.textContent = target.toLocaleString();
                clearInterval(timer);
            } else {
                el.textContent = Math.floor(current).toLocaleString();
            }
        }, 16);
    });
}

// HTMX loading indicator
document.addEventListener('htmx:beforeRequest', function(e) {
    const target = e.detail.target;
    if (target) {
        target.style.opacity = '0.5';
        target.style.transition = 'opacity 0.2s';
    }
});

document.addEventListener('htmx:afterSwap', function(e) {
    const target = e.detail.target;
    if (target) {
        target.style.opacity = '1';
    }

    // Update active filter tab
    const trigger = e.detail.requestConfig?.elt;
    if (trigger && trigger.classList.contains('filter-tab')) {
        trigger.closest('.filter-tabs')?.querySelectorAll('.filter-tab').forEach(t => t.classList.remove('active'));
        trigger.classList.add('active');
    }
});

// Init on page load
document.addEventListener('DOMContentLoaded', function() {
    animateCounters();

    // Close mobile sidebar on link click
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', () => {
            document.getElementById('sidebar')?.classList.remove('open');
        });
    });
});
