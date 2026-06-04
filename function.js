const quotes = [
    "Programming is the art of telling another human what one wants the computer to do.",
    "Typing fast requires practice and consistency every single day.",
    "Big data systems process huge amounts of information in real time.",
    "Kafka and Spark are powerful tools for streaming analytics.",
    "JavaScript makes websites interactive and dynamic.",
    "The future of web development lies in real-time applications and data streaming.",
    "Mastering keyboard typing can significantly boost your productivity as a developer."
];

const quoteElement = document.getElementById("quote");
const inputElement = document.getElementById("input");
const timeElement = document.getElementById("time");
const wpmElement = document.getElementById("wpm");
const accuracyElement = document.getElementById("accuracy");
const restartBtn = document.getElementById("restartBtn");
const simulateKafkaBtn = document.getElementById("simulateKafkaBtn");
const statusDot = document.getElementById("kafkaStatus");
const statusText = document.getElementById("statusText");
const userBadge = document.getElementById("userBadge");

let currentQuote = "";
let timer = 60;
let interval = null;
let started = false;
let kafkaActive = false;
let typingEvents = [];
let currentUser = null;
let sparkInterval = null;

// Socket.io connection to Kafka bridge
const socket = io('http://localhost:5000');

socket.on('connect', () => {
    console.log('✅ Connected to Kafka bridge');
    statusText.textContent = 'Connected to Kafka bridge';
});

socket.on('disconnect', () => {
    console.log('❌ Disconnected from Kafka bridge');
    if (kafkaActive) {
        statusText.textContent = '⚠️ Bridge disconnected!';
    }
});

// Check for saved user session on page load
window.addEventListener('load', () => {
    const savedUser = localStorage.getItem('currentUser');
    if (savedUser) {
        currentUser = JSON.parse(savedUser);
        updateUIForUser();
        resetTest();
    }
});

// Authentication System
function showAuthModal() {
    document.getElementById("authModal").style.display = "block";
}

function closeAuthModal() {
    document.getElementById("authModal").style.display = "none";
    document.getElementById('loginUsername').value = '';
    document.getElementById('loginPassword').value = '';
    document.getElementById('regUsername').value = '';
    document.getElementById('regEmail').value = '';
    document.getElementById('regPassword').value = '';
    document.getElementById('regConfirmPassword').value = '';
}

function switchTab(tab) {
    const tabs = document.querySelectorAll('.tab-btn');
    tabs.forEach(t => t.classList.remove('active'));
    if (tab === 'login') {
        tabs[0].classList.add('active');
        document.getElementById('loginForm').classList.add('active');
        document.getElementById('registerForm').classList.remove('active');
        document.getElementById('modalTitle').textContent = 'Sign In';
    } else {
        tabs[1].classList.add('active');
        document.getElementById('registerForm').classList.add('active');
        document.getElementById('loginForm').classList.remove('active');
        document.getElementById('modalTitle').textContent = 'Sign Up';
    }
}

// Add this INSIDE your login() function, right after setting currentUser and before closeAuthModal()
function login() {
    const username = document.getElementById('loginUsername').value.trim();
    const password = document.getElementById('loginPassword').value;
    
    if (!username || !password) {
        showError('Please fill in all fields');
        return;
    }
    
    const users = JSON.parse(localStorage.getItem('users') || '{}');
    if (users[username] && users[username].password === password) {
        currentUser = { username, email: users[username].email };
        localStorage.setItem('currentUser', JSON.stringify(currentUser));
        updateUIForUser();
        closeAuthModal();
        resetTest();
        statusText.textContent = `Welcome, ${username}!`;
        
        // ✅ ADD THESE TWO LINES:
        fetchSparkAnalytics();  // Fetch Spark stats for this user
        fetchLeaderboard();      // Fetch global leaderboard
        
    } else {
        showError('Invalid username or password');
    }
}

function register() {
    const username = document.getElementById('regUsername').value.trim();
    const email = document.getElementById('regEmail').value.trim();
    const password = document.getElementById('regPassword').value;
    const confirmPassword = document.getElementById('regConfirmPassword').value;
    
    if (!username || !email || !password || !confirmPassword) {
        showError('Please fill in all fields');
        return;
    }
    
    if (password !== confirmPassword) {
        showError('Passwords do not match');
        return;
    }
    
    if (password.length < 6) {
        showError('Password must be at least 6 characters');
        return;
    }
    
    if (!email.includes('@') || !email.includes('.')) {
        showError('Please enter a valid email address');
        return;
    }
    
    const users = JSON.parse(localStorage.getItem('users') || '{}');
    if (users[username]) {
        showError('Username already exists');
        return;
    }
    
    users[username] = { password, email };
    localStorage.setItem('users', JSON.stringify(users));
    alert('Registration successful! Please sign in.');
    switchTab('login');
}

function showError(message) {
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-message';
    errorDiv.textContent = message;
    errorDiv.style.cssText = `
        background: rgba(248, 113, 113, 0.2);
        color: #f87171;
        padding: 0.75rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        text-align: center;
    `;
    
    const activeForm = document.querySelector('.auth-form.active');
    const existingError = activeForm.querySelector('.error-message');
    if (existingError) existingError.remove();
    activeForm.insertBefore(errorDiv, activeForm.firstChild);
    
    setTimeout(() => {
        if (errorDiv.parentNode) errorDiv.remove();
    }, 3000);
}

function logout() {
    currentUser = null;
    localStorage.removeItem('currentUser');
    
    if (kafkaActive) {
        kafkaActive = false;
        if (sparkInterval) {
            clearInterval(sparkInterval);
            sparkInterval = null;
        }
        statusDot.classList.add('inactive');
        simulateKafkaBtn.innerHTML = `...`; // Keep your existing button HTML
    }
    
    updateUIForUser();
    resetTest();
    statusText.textContent = 'Signed out. Please sign in to start typing.';
}

function updateUIForUser() {
    if (currentUser) {
        userBadge.innerHTML = `👤 ${currentUser.username} ▼`;
        userBadge.onclick = () => {
            if (confirm(`Logged in as ${currentUser.username}\nDo you want to logout?`)) {
                logout();
            }
        };
        inputElement.disabled = false;
        restartBtn.disabled = false;
        simulateKafkaBtn.disabled = false;
        inputElement.placeholder = "Click here and start typing the passage above...";
    } else {
        userBadge.innerHTML = '👤 Sign In';
        userBadge.onclick = showAuthModal;
        inputElement.disabled = true;
        restartBtn.disabled = true;
        simulateKafkaBtn.disabled = true;
        inputElement.placeholder = "Please sign in to start typing...";
    }
}

function loadQuote() {
    const randomIndex = Math.floor(Math.random() * quotes.length);
    currentQuote = quotes[randomIndex];
    
    quoteElement.innerHTML = "";
    currentQuote.split("").forEach(char => {
        const span = document.createElement("span");
        if (char === ' ') {
            span.innerHTML = '&nbsp;';
            span.style.display = 'inline-block';
            span.style.minWidth = '0.5em';
        } else {
            span.innerText = char;
        }
        span.setAttribute('data-char', char === ' ' ? ' ' : char);
        quoteElement.appendChild(span);
    });
    
    const quoteLength = currentQuote.length;
    const lengthElement = document.querySelector('.quote-length');
    if (lengthElement) {
        lengthElement.textContent = `${quoteLength} characters (including spaces)`;
    }
}

function resetTest() {
    clearInterval(interval);
    
    timer = 60;
    started = false;
    timeElement.textContent = timer;
    wpmElement.textContent = 0;
    accuracyElement.textContent = 100;
    inputElement.value = "";
    inputElement.disabled = !currentUser;
    loadQuote();
    
    if (kafkaActive && currentUser) {
        typingEvents = [];
        sendToKafka("TEST_RESTART", { timestamp: Date.now() });
        statusText.textContent = "🔄 Restarted - Ready for new test";
    } else if (currentUser) {
        statusText.textContent = "Ready";
    }
}

// UPDATED: Send to REAL Kafka via bridge
function sendToKafka(eventType, data) {
    if (!kafkaActive || !currentUser) return;
    
    const event = {
        timestamp: new Date().toISOString(),
        eventType: eventType,
        userId: currentUser.username,
        data: data
    };
    
    // Send via WebSocket to bridge
    socket.emit('typing_event', event);
    
    typingEvents.push(event);
    statusText.textContent = `📡 Sent to Kafka (${typingEvents.length} events)`;
    console.log("[Kafka Sent]", event);
}

function simulateSparkProcessing() {
    // This is now handled by the real Spark
    console.log("Real Spark is processing data from Kafka");
}

inputElement.addEventListener("input", () => {
    if (!currentUser) {
        showAuthModal();
        return;
    }
    
    if (!started && timer > 0) {
        startTimer();
        started = true;
        sendToKafka("TEST_START", {
            quote: currentQuote,
            timestamp: Date.now()
        });
    }
    
    if (timer <= 0) return;
    
    const typedText = inputElement.value;
    const quoteSpans = quoteElement.querySelectorAll("span");
    let correctChars = 0;
    
    quoteSpans.forEach((charSpan, index) => {
        const expectedChar = charSpan.getAttribute('data-char') || charSpan.innerText;
        const typedChar = typedText[index];
        
        if (typedChar == null) {
            charSpan.classList.remove("correct");
            charSpan.classList.remove("incorrect");
        } else if (typedChar === expectedChar) {
            charSpan.classList.add("correct");
            charSpan.classList.remove("incorrect");
            correctChars++;
        } else {
            charSpan.classList.add("incorrect");
            charSpan.classList.remove("correct");
        }
    });
    
    const accuracy = typedText.length > 0
        ? Math.round((correctChars / typedText.length) * 100)
        : 100;
    
    accuracyElement.textContent = accuracy;
    
    const charactersTyped = typedText.length;
    const wordsTyped = charactersTyped / 5;
    const timePassed = 60 - timer;
    const minutes = timePassed / 60;
    
    let wpm = 0;
    if (minutes > 0) {
        wpm = Math.round(wordsTyped / minutes);
        wpmElement.textContent = wpm;
    }
    
    // Send keystroke to Kafka
    if (kafkaActive && started && timer > 0) {
        sendToKafka("KEYSTROKE", {
            correct: correctChars,
            total: typedText.length,
            accuracy: accuracy,
            wpm: wpm,
            timestamp: Date.now()
        });
    }
    
    if (typedText === currentQuote && timer > 0) {
        clearInterval(interval);
        inputElement.disabled = true;
        started = false;
        
        sendToKafka("TEST_COMPLETE", {
            finalWPM: wpm,
            finalAccuracy: accuracy,
            timeRemaining: timer,
            quoteLength: currentQuote.length
        });
        
        quoteElement.innerHTML = `
            <span style="color: #4ade80; text-shadow: 0 0 10px rgba(74,222,128,0.5); display: block; text-align: center;">
                🎉 Test Complete! Amazing job, ${currentUser.username}! 🎉<br>
                WPM: ${wpm} | Accuracy: ${accuracy}%
            </span>
        `;
        
        statusText.textContent = `✅ Test completed! ${typingEvents.length} events sent to Kafka`;
    }
});

function startTimer() {
    interval = setInterval(() => {
        if (timer > 0) {
            timer--;
            timeElement.textContent = timer;
        }
        
        if (timer <= 0) {
            clearInterval(interval);
            inputElement.disabled = true;
            started = false;
            
            const finalWPM = parseInt(wpmElement.textContent);
            const finalAccuracy = parseInt(accuracyElement.textContent);
            
            sendToKafka("TEST_TIMEOUT", {
                finalWPM: finalWPM,
                finalAccuracy: finalAccuracy,
                timestamp: Date.now()
            });
            
            quoteElement.innerHTML = `
                <span style="color: #f87171; display: block; text-align: center;">
                    ⏰ Time's up!<br>
                    Final WPM: ${finalWPM} | Accuracy: ${finalAccuracy}%<br>
                    Click 'New Challenge' to try again!
                </span>
            `;
            
            statusText.textContent = `⏰ Timeout! ${typingEvents.length} total events`;
        }
    }, 1000);
}

restartBtn.addEventListener("click", () => {
    if (!currentUser) {
        showAuthModal();
        return;
    }
    resetTest();
});

simulateKafkaBtn.addEventListener("click", () => {
    if (!currentUser) {
        showAuthModal();
        return;
    }
    
    kafkaActive = !kafkaActive;
    
    if (kafkaActive) {
        statusDot.classList.remove("inactive");
        statusText.textContent = "📡 Connected to Kafka - Streaming events";
        simulateKafkaBtn.innerHTML = `Stop Kafka Stream`;
        
        typingEvents = [];
        
        sendToKafka("SESSION_START", {
            timestamp: Date.now(),
            userAgent: navigator.userAgent
        });
    } else {
        statusDot.classList.add("inactive");
        statusText.textContent = "⏸️ Kafka disconnected";
        simulateKafkaBtn.innerHTML = `Start Kafka Stream`;
    }
});

// Initialize
loadQuote();

window.onclick = function(event) {
    const modal = document.getElementById('authModal');
    if (event.target === modal) {
        closeAuthModal();
    }
};


// Add this to your function.js file

// Fetch Spark analytics for current user
async function fetchSparkAnalytics() {
    if (!currentUser) return;
    
    try {
        const response = await fetch(`/api/stats/${currentUser.username}`);
        const stats = await response.json();
        
        // Display Spark analytics in UI
        updateSparkAnalyticsDisplay(stats);
    } catch (error) {
        console.log('Spark analytics not available yet');
    }
}

// Display Spark analytics in dashboard
function updateSparkAnalyticsDisplay(stats) {
    let sparkCard = document.getElementById('spark-analytics');
    
    if (!sparkCard && stats.avg_wpm > 0) {
        // Create new analytics card
        const statsPanel = document.querySelector('.stats-panel');
        sparkCard = document.createElement('div');
        sparkCard.id = 'spark-analytics';
        sparkCard.className = 'stat-card';
        sparkCard.style.background = 'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)';
        sparkCard.innerHTML = `
            <div class="stat-icon">🤖</div>
            <div class="stat-content">
                <div class="stat-label">Spark Analytics</div>
                <div class="stat-value">${stats.avg_wpm}<span class="stat-unit">WPM avg</span></div>
                <div class="stat-label" style="font-size: 0.7rem;">Rank: ${stats.global_rank}</div>
            </div>
        `;
        statsPanel.appendChild(sparkCard);
    } else if (sparkCard && stats.avg_wpm > 0) {
        // Update existing card
        sparkCard.querySelector('.stat-value').innerHTML = `${stats.avg_wpm}<span class="stat-unit">WPM avg</span>`;
        sparkCard.querySelector('.stat-label:last-child').textContent = `Rank: ${stats.global_rank} | ${stats.performance_trend}`;
    }
}

// Fetch and display leaderboard
async function fetchLeaderboard() {
    try {
        const response = await fetch('/api/leaderboard');
        const leaderboard = await response.json();
        
        let leaderboardCard = document.getElementById('leaderboard');
        if (!leaderboardCard && leaderboard.length > 0) {
            // Create leaderboard section
            const container = document.querySelector('.container');
            const leaderboardHTML = `
                <div id="leaderboard" class="quote-container" style="margin-top: 1rem;">
                    <div class="quote-header">
                        <span>🏆 Global Leaderboard (Spark Analytics)</span>
                    </div>
                    <div id="leaderboard-list" style="color: white;">
                        ${leaderboard.map((user, idx) => `
                            <div style="display: flex; justify-content: space-between; padding: 0.5rem; border-bottom: 1px solid rgba(255,255,255,0.1);">
                                <span>${idx + 1}. ${user.username}</span>
                                <span>${user.avg_wpm} WPM (${user.avg_accuracy}% acc)</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
            container.insertAdjacentHTML('beforeend', leaderboardHTML);
        }
    } catch (error) {
        console.log('Leaderboard not available yet');
    }
}

// Socket event listeners for Spark updates
socket.on('spark_update', (data) => {
    if (data.userId === currentUser?.username) {
        updateSparkAnalyticsDisplay(data.stats);
    }
    // Refresh leaderboard on any update
    fetchLeaderboard();
});

// Call these when user logs in
// Add to your login() function after setting currentUser:
// fetchSparkAnalytics();
// fetchLeaderboard();

// Call periodically for updates
if (currentUser) {
    setInterval(fetchSparkAnalytics, 30000); // Every 30 seconds
    setInterval(fetchLeaderboard, 60000); // Every minute
}

// Add this at the VERY END of your function.js file

// Initialize analytics when page loads and user is already logged in
window.addEventListener('load', () => {
    const savedUser = localStorage.getItem('currentUser');
    if (savedUser) {
        currentUser = JSON.parse(savedUser);
        updateUIForUser();
        resetTest();
        
        // ✅ Add these lines for analytics on page load
        setTimeout(() => {
            fetchSparkAnalytics();
            fetchLeaderboard();
        }, 1000); // Delay 1 second to ensure everything is ready
    }
});

// Also listen for leaderboard updates from Spark
socket.on('leaderboard_update', (leaderboardData) => {
    console.log('Leaderboard updated:', leaderboardData);
    updateLeaderboardDisplay(leaderboardData);
});

// Function to update leaderboard display
function updateLeaderboardDisplay(leaderboardData) {
    const leaderboardDiv = document.getElementById('leaderboard-list');
    if (leaderboardDiv && leaderboardData.length > 0) {
        leaderboardDiv.innerHTML = leaderboardData.map((user, idx) => `
            <div style="display: flex; justify-content: space-between; padding: 0.5rem; border-bottom: 1px solid rgba(255,255,255,0.1);">
                <span>${idx + 1}. ${user.userId}</span>
                <span>${user.avg_wpm} WPM (${user.avg_accuracy}% acc)</span>
            </div>
        `).join('');
    }
}