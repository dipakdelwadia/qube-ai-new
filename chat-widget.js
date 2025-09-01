(function() {
    // Self-executing anonymous function to avoid polluting the global namespace

    // 1. DEFINE GLOBAL VARIABLES
    let chatButton = null;
    let chatWindow = null;
    let messageArea = null;
    let inputField = null;
    let sendButton = null;
    let closeButton = null;
    let resizers = [];
    let initialResizeState = null;
    let isProcessing = false;
    let currentAbortController = null;
    let lastUserMessageContainer = null;
    let activeEditableBubble = null;
    
    // Add conversation history variable
    let conversationHistory = [];
    // Chart counter to ensure unique IDs
    let chartCounter = 0;
    // Toggle for chart visualization
    let showChartVisualization = false;

    // 2. INITIALIZATION FUNCTION
    function initChatWidget() {
        // Get references to the elements from the DOM
        chatButton = document.getElementById("ai-chat-widget-button");
        chatWindow = document.getElementById("ai-chat-widget-window");
        messageArea = document.getElementById("ai-chat-widget-messages");
        inputField = document.querySelector(".ai-chat-widget-input-area input");
        sendButton = document.getElementById("ai-chat-widget-send-button");
        closeButton = document.getElementById("ai-chat-widget-close-button");
        resizers = document.querySelectorAll(".ai-chat-widget-resizer");

        console.log("Chat widget initialized.");
    }

    // 3. EVENT LISTENERS
    function addEventListeners() {
        if (chatButton) {
            chatButton.addEventListener("click", toggleChatWindow);
        }
        if (sendButton) {
            sendButton.addEventListener("click", handleSendButtonClick);
        }
        if (inputField) {
            inputField.addEventListener("keydown", (event) => {
                if (event.key === "Enter") {
                    if (isProcessing) {
                        // Ignore Enter while processing
                        event.preventDefault();
                        return;
                    }
                    // If editing from bubble, Enter will be handled on the bubble itself
                    if (!activeEditableBubble) {
                        sendMessage();
                    }
                }
            });
        }
        if (closeButton) {
            closeButton.addEventListener("click", () => {
                if (chatWindow) {
                    chatWindow.classList.remove("visible");
                }
            });
        }
        if (resizers.length > 0) {
            resizers.forEach(resizer => {
                resizer.addEventListener("mousedown", initResize);
            });
        }
        
        // Add event listener for chart dropdown
        const chartDropdown = document.getElementById("ai-chat-widget-chart-dropdown");
        const chartDropdownBtn = document.getElementById("ai-chat-widget-chart-dropdown-btn");
        const chartDropdownMenu = document.getElementById("ai-chat-widget-chart-dropdown-menu");
        
        if (chartDropdownBtn && chartDropdown && chartDropdownMenu) {
            chartDropdownBtn.addEventListener("click", toggleChartDropdown);
            
            // Close dropdown when clicking outside
            document.addEventListener("click", (event) => {
                if (!chartDropdown.contains(event.target)) {
                    closeChartDropdown();
                }
            });
            
            // Handle dropdown item clicks
            chartDropdownMenu.addEventListener("click", handleChartDropdownAction);
        }
        
        // Add event listener for help button
        const helpButton = document.getElementById("ai-chat-widget-help-button");
        if (helpButton) {
            helpButton.addEventListener("click", showHelpMessage);
        }
        
        // Add event listener for refresh button
        const refreshButton = document.getElementById("ai-chat-widget-refresh-button");
        if (refreshButton) {
            refreshButton.addEventListener("click", clearConversationHistory);
        }
    }

    function setProcessingState(processing) {
        isProcessing = processing;
        if (sendButton) {
            if (processing) {
                sendButton.textContent = "Stop";
                sendButton.setAttribute('data-state', 'processing');
            } else {
                sendButton.textContent = "Send";
                sendButton.removeAttribute('data-state');
            }
        }
    }

    function handleSendButtonClick() {
        if (isProcessing) {
            stopRequest();
            return;
        }
        if (activeEditableBubble) {
            const edited = activeEditableBubble.innerText.trim();
            if (!edited) return;
            // Push user message to history (we popped it on cancel)
            conversationHistory.push({ role: 'user', content: edited });
            // Remove edit toolbar and disable editing
            removeEditToolbar(lastUserMessageContainer);
            activeEditableBubble.contentEditable = "false";
            activeEditableBubble = null;
            // Start request using the edited text
            startRequest(edited);
            return;
        }
        // Default: send from input
        sendMessage();
    }

    function stopRequest() {
        try {
            if (currentAbortController) {
                currentAbortController.abort();
            }
        } catch (_) {}
        displayLoading(false);
        setProcessingState(false);
        // Remove the last user message from history since it was cancelled
        if (conversationHistory.length > 0 && conversationHistory[conversationHistory.length - 1].role === 'user') {
            conversationHistory.pop();
        }
        // Make the last user message editable and show edit/copy controls
        if (lastUserMessageContainer) {
            makeMessageEditable(lastUserMessageContainer);
        }
    }

    function ensureUserActions(container) {
        if (!container) return;
        let actions = container.querySelector('.ai-chat-widget-message-actions');
        if (!actions) {
            actions = document.createElement('div');
            actions.className = 'ai-chat-widget-message-actions';
            container.appendChild(actions);
        }
        // Ensure copy button exists (always available)
        if (!actions.querySelector('.ai-chat-copy-button')) {
            const copyBtn = document.createElement('button');
            copyBtn.className = 'ai-chat-widget-action-button ai-chat-copy-button';
            copyBtn.title = 'Copy';
            copyBtn.innerHTML = '<i class="far fa-copy"></i>';
            copyBtn.addEventListener('click', async (e) => {
                e.stopPropagation();
                const bubble = container.querySelector('.ai-chat-widget-message');
                if (!bubble) return;
                try { await navigator.clipboard.writeText(bubble.innerText); } catch (_) {}
            });
            actions.appendChild(copyBtn);
        }
    }

    function addEditButton(container) {
        if (!container) return;
        const actions = container.querySelector('.ai-chat-widget-message-actions');
        if (!actions) return;
        if (actions.querySelector('.ai-chat-edit-button')) return;
        const editBtn = document.createElement('button');
        editBtn.className = 'ai-chat-widget-action-button ai-chat-edit-button';
        editBtn.title = 'Edit';
        editBtn.innerHTML = '<i class="far fa-pen-to-square"></i>';
        editBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            enableEditing(container);
        });
        actions.appendChild(editBtn);
    }

    function enableEditing(container) {
        const bubble = container.querySelector('.ai-chat-widget-message');
        if (!bubble) return;
        bubble.contentEditable = "true";
        bubble.classList.add('is-editing');
        bubble.focus();
        
        // Move cursor to the end of the text
        const range = document.createRange();
        const selection = window.getSelection();
        range.selectNodeContents(bubble);
        range.collapse(false); // false = collapse to end
        selection.removeAllRanges();
        selection.addRange(range);
        
        activeEditableBubble = bubble;
        bubble.addEventListener('keydown', onEditableBubbleKeydown);
    }

    function makeMessageEditable(container) {
        const bubble = container.querySelector('.ai-chat-widget-message');
        if (!bubble) return;
        // After stop, show the edit button (copy is already present)
        ensureUserActions(container);
        addEditButton(container);
    }

    function removeEditToolbar(container) {
        if (!container) return;
        const bubble = container.querySelector('.ai-chat-widget-message');
        if (bubble) {
            bubble.removeEventListener('keydown', onEditableBubbleKeydown);
            bubble.classList.remove('is-editing');
        }
        // Remove edit button; keep copy button
        const editBtn = container.querySelector('.ai-chat-edit-button');
        if (editBtn) editBtn.remove();
    }

    function onEditableBubbleKeydown(event) {
        if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault();
            if (!activeEditableBubble) return;
            const edited = activeEditableBubble.innerText.trim();
            if (!edited) return;
            // Push user message and start request
            conversationHistory.push({ role: 'user', content: edited });
            removeEditToolbar(lastUserMessageContainer);
            activeEditableBubble.contentEditable = "false";
            activeEditableBubble = null;
            startRequest(edited);
        }
    }

    function initResize(e) {
        e.preventDefault();
        const direction = e.target.getAttribute("data-direction");
        if (!direction) return;

        initialResizeState = {
            width: chatWindow.offsetWidth,
            height: chatWindow.offsetHeight,
            x: e.clientX,
            y: e.clientY,
            direction: direction
        };
        window.addEventListener("mousemove", startResizing);
        window.addEventListener("mouseup", stopResizing);
    }

    function startResizing(e) {
        if (!chatWindow || !initialResizeState) return;

        const dx = e.clientX - initialResizeState.x;
        const dy = e.clientY - initialResizeState.y;
        let newWidth = initialResizeState.width;
        let newHeight = initialResizeState.height;

        const direction = initialResizeState.direction;

        if (direction.includes("e")) {
            newWidth = initialResizeState.width + dx;
        } else if (direction.includes("w")) {
            newWidth = initialResizeState.width - dx;
        }

        if (direction.includes("s")) {
            newHeight = initialResizeState.height + dy;
        } else if (direction.includes("n")) {
            newHeight = initialResizeState.height - dy;
        }
        
        // Enforce minimum dimensions
        const minWidth = 280;
        const minHeight = 300;

        if (newWidth < minWidth) {
            newWidth = minWidth;
        }
        if (newHeight < minHeight) {
            newHeight = minHeight;
        }

        chatWindow.style.width = `${newWidth}px`;
        chatWindow.style.height = `${newHeight}px`;
    }

    function stopResizing() {
        window.removeEventListener("mousemove", startResizing);
        window.removeEventListener("mouseup", stopResizing);
        initialResizeState = null;
    }

    function toggleChatWindow() {
        if (chatWindow) {
            // const isHidden = chatWindow.style.display === "none" || chatWindow.style.display === "";
            // chatWindow.style.display = isHidden ? "flex" : "none";
            chatWindow.classList.toggle("visible");
        }
    }

    // New functions to handle chart dropdown
    function toggleChartDropdown() {
        const chartDropdown = document.getElementById("ai-chat-widget-chart-dropdown");
        chartDropdown.classList.toggle("open");
    }
    
    function closeChartDropdown() {
        const chartDropdown = document.getElementById("ai-chat-widget-chart-dropdown");
        chartDropdown.classList.remove("open");
    }
    
    function handleChartDropdownAction(event) {
        const item = event.target.closest('.ai-chat-widget-chart-dropdown-item');
        if (!item) return;
        
        const action = item.getAttribute('data-action');
        
        switch (action) {
            case 'toggle-visualization':
                toggleChartVisualization();
                break;
            case 'export-data':
                handleExportData();
                break;
            case 'chart-settings':
                handleChartSettings();
                break;
        }
        
        closeChartDropdown();
    }
    
    function toggleChartVisualization() {
        const chartDropdownBtn = document.getElementById("ai-chat-widget-chart-dropdown-btn");
        const visualizationItem = document.querySelector('[data-action="toggle-visualization"]');
        
        showChartVisualization = !showChartVisualization;
        
        if (showChartVisualization) {
            chartDropdownBtn.classList.add("active");
            chartDropdownBtn.setAttribute("title", "Chart visualization enabled");
            visualizationItem.classList.add("visualization-active");
            visualizationItem.querySelector('span').textContent = "Hide chart visualization";
        } else {
            chartDropdownBtn.classList.remove("active");
            chartDropdownBtn.setAttribute("title", "Chart visualization disabled");
            visualizationItem.classList.remove("visualization-active");
            visualizationItem.querySelector('span').textContent = "Show chart visualization";
        }
    }
    
    function handleExportData() {
        // Placeholder for export data functionality
        console.log("Export data functionality to be implemented");
        // You can implement actual export logic here
    }
    
    function handleChartSettings() {
        // Placeholder for chart settings functionality
        console.log("Chart settings functionality to be implemented");
        // You can implement chart settings modal/panel here
    }
    
    function showHelpMessage() {
        const helpText = `✨ QubeAI

What I do:
- Answer natural-language questions with live data
- Auto-build charts and key insights
- Export tables and visuals (CSV/PNG/PDF)

Quick tips:
- Add timeframes: "last 30 days", "2025 Q1"
- Group/compare: "by customer", "billable vs unbillable by month"
- Say "show a chart" or use the ⚡Action menu to keep charts on

Pro move: Click any related question below a chart to drill deeper.`;

        displayMessage(helpText, "ai", true);
    }
    
    function clearConversationHistory() {
        // Clear the conversation history array
        conversationHistory = [];
        
        // Clear all messages from the message area and restore the welcome message
        if (messageArea) {
            messageArea.innerHTML = `
                <div class="ai-chat-widget-message-container ai">
                    <div class="ai-chat-widget-message ai">
                        Welcome! How can I help you with your query today?
                    </div>
                    <div class="ai-chat-widget-avatar">
                        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" fill="currentColor" class="bi bi-robot" viewBox="0 0 16 16">
                            <path d="M6 12.5a.5.5 0 0 1 .5-.5h3a.5.5 0 0 1 0 1h-3a.5.5 0 0 1-.5-.5M3 8.062C3 6.76 4.235 5.765 5.53 5.886a26.6 26.6 0 0 0 4.94 0C11.765 5.765 13 6.76 13 8.062v1.157a.93.93 0 0 1-.765.935c-.845.147-2.34.346-4.235.346s-3.39-.2-4.235-.346A.93.93 0 0 1 3 9.219zm4.542-.827a.25.25 0 0 0-.217.068l-.92.9a25 25 0 0 1-1.871-.183.25.25 0 0 0-.068.495c.55.076 1.232.149 2.02.193a.25.25 0 0 0 .189-.071l.754-.736.847 1.71a.25.25 0 0 0 .404.062l.932-.97a25 25 0 0 0 1.922-.188.25.25 0 0 0-.068-.495c-.538.074-1.207.145-1.98.189a.25.25 0 0 0-.166.076l-.754.785-.842-1.7a.25.25 0 0 0-.182-.135"/>
                            <path d="M8.5 1.866a1 1 0 1 0-1 0V3h-2A4.5 4.5 0 0 0 1 7.5V8a1 1 0 0 0-1 1v2a1 1 0 0 0 1 1v1a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-1a1 1 0 0 0 1-1V9a1 1 0 0 0-1-1v-.5A4.5 4.5 0 0 0 10.5 3h-2zM14 7.5V13a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V7.5A3.5 3.5 0 0 1 5.5 4h5A3.5 3.5 0 0 1 14 7.5"/>
                        </svg>
                    </div>
                </div>
            `;
        }
        
        // Reset chart counter
        chartCounter = 0;
        
        // Show a brief confirmation message
        console.log("Conversation history cleared");
        
        // Optional: Add a subtle visual feedback
        const refreshButton = document.getElementById("ai-chat-widget-refresh-button");
        if (refreshButton) {
            const originalIcon = refreshButton.innerHTML;
            refreshButton.innerHTML = '<i class="fas fa-check"></i>';
            refreshButton.style.backgroundColor = 'rgba(34, 197, 94, 1)';
            
            setTimeout(() => {
                refreshButton.innerHTML = originalIcon;
                refreshButton.style.backgroundColor = 'rgba(34, 197, 94, 0.8)';
            }, 500);
        }
    }

    // Function to detect if user query contains chart-related keywords
    function detectChartRequest(query) {
        const chartKeywords = [
            // Chart types
            'chart', 'graph', 'plot', 'visualization', 'visualize', 'visual',
            
            // Specific chart types
            'bar chart', 'line chart', 'pie chart', 'scatter plot', 'histogram', 
            'area chart', 'donut chart', 'bubble chart',
            
            // Action words for charts
            'show', 'display', 'create', 'generate', 'draw', 'make', 'build',
            
            // Combined phrases
            'show me a chart', 'create a graph', 'generate visualization', 
            'display chart', 'make a plot', 'draw a graph', 'build a chart',
            'show visualization', 'create visualization', 'display graph',
            'show graph', 'make graph', 'generate chart', 'generate graph',
            
            // Trending/analysis terms often used with charts
            'trend', 'trending', 'pattern', 'growth', 'comparison', 'compare',
            'over time', 'by month', 'by year', 'by quarter', 'distribution'
        ];
        
        const queryLower = query.toLowerCase();
        
        // Check for exact phrase matches first (more specific)
        const phrases = [
            'show me a chart', 'create a graph', 'generate visualization', 
            'display chart', 'make a plot', 'draw a graph', 'build a chart',
            'show visualization', 'create visualization', 'display graph',
            'show graph', 'make graph', 'generate chart', 'generate graph',
            'bar chart', 'line chart', 'pie chart', 'scatter plot', 
            'area chart', 'donut chart', 'bubble chart', 'over time'
        ];
        
        for (const phrase of phrases) {
            if (queryLower.includes(phrase)) {
                return true;
            }
        }
        
        // Check for individual keywords with context
        for (const keyword of chartKeywords) {
            if (queryLower.includes(keyword)) {
                // Additional context checking for common words like 'show'
                if (keyword === 'show' || keyword === 'display' || keyword === 'create' || keyword === 'generate' || keyword === 'make') {
                    // These words should be followed by chart-related terms
                    const chartRelated = ['chart', 'graph', 'plot', 'visualization', 'visual', 'trend', 'pattern'];
                    const hasChartContext = chartRelated.some(term => queryLower.includes(term));
                    if (hasChartContext) {
                        return true;
                    }
                } else {
                    // Direct chart-related keywords
                    return true;
                }
            }
        }
        
        return false;
    }

    // 4. API COMMUNICATION
    async function sendMessage() {
        const query = inputField.value.trim();
        if (query === "") return;

        // Detect if the user is requesting a chart
        const isChartRequest = detectChartRequest(query);
        
        // Determine if charts should be shown for this request
        // Either the toggle is enabled OR the user specifically requested a chart
        const shouldShowCharts = showChartVisualization || isChartRequest;

        // Add user message to UI and keep reference
        lastUserMessageContainer = displayMessage(query, "user");
        inputField.value = "";
        
        displayLoading(true);
        setProcessingState(true);

        try {
            currentAbortController = new AbortController();
            // Send query and conversation history to the backend
            const response = await fetch('http://127.0.0.1:8000/api/ask', {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'X-OpsFlo-Env': 'PRID-UAT' // Set environment to NEWDEMO to test connection
                },
                signal: currentAbortController.signal,
                body: JSON.stringify({ 
                    query: query,
                    conversation_history: conversationHistory,
                    show_charts: shouldShowCharts // Use dynamic chart detection or toggle state
                })
            });

            displayLoading(false);
            setProcessingState(false);
            currentAbortController = null;

            if (!response.ok) {
                const errorData = await response.json();
                displayMessage(errorData.detail || "An unknown error occurred.", "ai");
                
                // Add error to conversation history
                conversationHistory.push({
                    role: "assistant",
                    content: errorData.detail || "An unknown error occurred."
                });
                
                return;
            }

            const result = await response.json();
            
            // Create a single container for the entire AI response turn
            const aiMessageGroup = document.createElement("div");
            aiMessageGroup.className = "ai-chat-widget-message-container ai";
            // Keep a reference to the AI avatar so we can reposition it later
            let avatarEl = null;

            // Update conversation history with the latest from the server
            if (result.conversation_history && result.conversation_history.length > 0) {
                conversationHistory = result.conversation_history;
            }

            // Handle different response types by adding message bubbles to the group
            if (result.error) {
                appendMessageBubble(aiMessageGroup, result.error);
                conversationHistory.push({ role: 'assistant', content: 'An error occurred. Please check the logs.' });
            } else if (result.question) {
                appendMessageBubble(aiMessageGroup, result.question);
            } else if (result.data && result.data.length > 0) {
                // Check if we have chart data and charts are enabled
                if (result.chart && shouldShowCharts) {
                    // Add intro message with consistent styling
                    appendIntroMessageBubble(aiMessageGroup, "Here's a visualization of your data:");
                    
                    // Create a visible data table container that will be toggled
                    const tableContainer = document.createElement("div");
                    tableContainer.style.display = "block"; // Initially visible (changed from "none")
                    appendTableBubble(tableContainer, result.data);
                    
                    // Add the chart (pass tableContainer to handle toggling)
                    appendChartBubble(aiMessageGroup, result.chart, tableContainer);
                    
                    // If background insights are scheduled, poll and append when ready
                    if (result.request_id) {
                        const insightsContainer = document.createElement('div');
                        insightsContainer.className = 'ai-chat-widget-insights-pending';
                        // Enhanced loading UI
                        const loadingWrapper = document.createElement('div');
                        loadingWrapper.className = 'ai-chat-widget-message ai ai-chat-insights-loading';
                        loadingWrapper.innerHTML = `
                            <div class="ai-chat-insights-loading-header">
                                <span class="ai-chat-spinner"></span>
                                <strong>Preparing Insights</strong>
                            </div>
                            <ul class="ai-chat-insights-steps">
                                <li class="ai-chat-insights-step active"><span class="dot"></span>Analyzing the chart...</li>
                                <li class="ai-chat-insights-step"><span class="dot"></span>Looking for data points...</li>
                                <li class="ai-chat-insights-step"><span class="dot"></span>Generating insights...</li>
                                <li class="ai-chat-insights-step"><span class="dot"></span>Generating related questions...</li>
                                <li class="ai-chat-insights-step"><span class="dot"></span>Loading insights...</li>
                            </ul>
                            <div class="ai-chat-insights-progress"><div class="ai-chat-insights-progress-bar"></div></div>
                        `;
                        insightsContainer.appendChild(loadingWrapper);
                        aiMessageGroup.appendChild(insightsContainer);

                        // Cycle through steps
                        const steps = loadingWrapper.querySelectorAll('.ai-chat-insights-step');
                        const progressBar = loadingWrapper.querySelector('.ai-chat-insights-progress-bar');
                        let stepIndex = 0;
                        const totalSteps = steps.length;
                        const stepTimer = setInterval(() => {
                            steps.forEach((s, i) => {
                                s.classList.remove('active');
                                if (i < stepIndex) s.classList.add('done');
                            });
                            if (steps[stepIndex]) {
                                steps[stepIndex].classList.add('active');
                            }
                            progressBar.style.width = `${Math.min(100, Math.round(((stepIndex+1)/totalSteps)*100))}%`;
                            stepIndex = (stepIndex + 1) % totalSteps;
                        }, 900);

                        const requestId = result.request_id;
                        let attempts = 0;
                        const maxAttempts = 20; // ~20 * 1s = 20s
                        const poll = async () => {
                            try {
                                const r = await fetch(`http://127.0.0.1:8000/api/insights/${requestId}`);
                                const j = await r.json();
                                if (j.status === 'ready') {
                                    clearInterval(stepTimer);
                                    insightsContainer.innerHTML = '';
                                    if (j.insights) appendInsightsBubble(insightsContainer, j.insights);
                                    if (j.follow_up_questions && j.follow_up_questions.length > 0) {
                                        appendFollowUpQuestionsBubble(aiMessageGroup, j.follow_up_questions);
                                        if (avatarEl && avatarEl.parentElement === aiMessageGroup) {
                                            aiMessageGroup.appendChild(avatarEl);
                                        }
                                    }
                                    aiMessageGroup.scrollIntoView({ behavior: 'smooth', block: 'start' });
                                    return;
                                } else if (j.status === 'error') {
                                    clearInterval(stepTimer);
                                    insightsContainer.innerHTML = '';
                                    appendMessageBubble(insightsContainer, 'Insights unavailable.');
                                    return;
                                }
                            } catch (e) {
                                // ignore and keep polling a bit
                            }
                            attempts++;
                            if (attempts < maxAttempts) {
                                setTimeout(poll, 1000);
                            } else {
                                clearInterval(stepTimer);
                                insightsContainer.innerHTML = '';
                                appendMessageBubble(insightsContainer, 'Insights took too long to generate.');
                            }
                        };
                        setTimeout(poll, 1000);
                    }
                    
                    // Add the data table container
                    aiMessageGroup.appendChild(tableContainer);
                } else {
                    // Just show the table as usual with consistent intro styling
                    appendIntroMessageBubble(aiMessageGroup, "Here are the results for your query:");
                    appendTableBubble(aiMessageGroup, result.data);
                }
            } else {
                appendMessageBubble(aiMessageGroup, "No results found for your query.");
            }
            
            // Add the single avatar to the group and append to the message area
            avatarEl = createAIAvatar();
            aiMessageGroup.appendChild(avatarEl);
            messageArea.appendChild(aiMessageGroup);
            aiMessageGroup.scrollIntoView({ behavior: 'smooth', block: 'start' });

        } catch (error) {
            displayLoading(false);
            setProcessingState(false);
            const aborted = error && (error.name === 'AbortError' || error.code === 20);
            if (aborted) {
                // Already handled by stopRequest; ensure UI state
                return;
            }
            displayMessage("Sorry, could not connect to the AI service. Please ensure the backend is running.", "ai");
            console.error("Error sending message:", error);
        }
    }

    // Start a request using an existing edited bubble (no new user bubble needed)
    async function startRequest(query) {
        const isChartRequest = detectChartRequest(query);
        const shouldShowCharts = showChartVisualization || isChartRequest;
        displayLoading(true);
        setProcessingState(true);
        try {
            currentAbortController = new AbortController();
            const response = await fetch('http://127.0.0.1:8000/api/ask', {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'X-OpsFlo-Env': 'PRID-UAT'
                },
                signal: currentAbortController.signal,
                body: JSON.stringify({ 
                    query: query,
                    conversation_history: conversationHistory,
                    show_charts: shouldShowCharts
                })
            });

            displayLoading(false);
            setProcessingState(false);
            currentAbortController = null;

            if (!response.ok) {
                const errorData = await response.json();
                displayMessage(errorData.detail || "An unknown error occurred.", "ai");
                conversationHistory.push({ role: 'assistant', content: errorData.detail || 'An unknown error occurred.' });
                return;
            }

            const result = await response.json();
            const aiMessageGroup = document.createElement('div');
            aiMessageGroup.className = 'ai-chat-widget-message-container ai';
            let avatarEl = null;

            if (result.conversation_history && result.conversation_history.length > 0) {
                conversationHistory = result.conversation_history;
            }

            if (result.error) {
                appendMessageBubble(aiMessageGroup, result.error);
                conversationHistory.push({ role: 'assistant', content: 'An error occurred. Please check the logs.' });
            } else if (result.question) {
                appendMessageBubble(aiMessageGroup, result.question);
            } else if (result.data && result.data.length > 0) {
                if (result.chart && (showChartVisualization || isChartRequest)) {
                    appendIntroMessageBubble(aiMessageGroup, "Here's a visualization of your data:");
                    const tableContainer = document.createElement('div');
                    tableContainer.style.display = 'block';
                    appendTableBubble(tableContainer, result.data);
                    appendChartBubble(aiMessageGroup, result.chart, tableContainer);
                    if (result.request_id) {
                        const insightsContainer = document.createElement('div');
                        insightsContainer.className = 'ai-chat-widget-insights-pending';
                        const loadingWrapper = document.createElement('div');
                        loadingWrapper.className = 'ai-chat-widget-message ai ai-chat-insights-loading';
                        loadingWrapper.innerHTML = `
                            <div class="ai-chat-insights-loading-header">
                                <span class="ai-chat-spinner"></span>
                                <strong>Preparing Insights</strong>
                            </div>
                            <ul class="ai-chat-insights-steps">
                                <li class="ai-chat-insights-step active"><span class="dot"></span>Analyzing the chart...</li>
                                <li class="ai-chat-insights-step"><span class="dot"></span>Looking for data points...</li>
                                <li class="ai-chat-insights-step"><span class="dot"></span>Generating insights...</li>
                                <li class="ai-chat-insights-step"><span class="dot"></span>Generating related questions...</li>
                                <li class="ai-chat-insights-step"><span class="dot"></span>Loading insights...</li>
                            </ul>
                            <div class="ai-chat-insights-progress"><div class="ai-chat-insights-progress-bar"></div></div>
                        `;
                        insightsContainer.appendChild(loadingWrapper);
                        aiMessageGroup.appendChild(insightsContainer);
                        const steps = loadingWrapper.querySelectorAll('.ai-chat-insights-step');
                        const progressBar = loadingWrapper.querySelector('.ai-chat-insights-progress-bar');
                        let stepIndex = 0;
                        const totalSteps = steps.length;
                        const stepTimer = setInterval(() => {
                            steps.forEach((s, i) => {
                                s.classList.remove('active');
                                if (i < stepIndex) s.classList.add('done');
                            });
                            if (steps[stepIndex]) steps[stepIndex].classList.add('active');
                            progressBar.style.width = `${Math.min(100, Math.round(((stepIndex+1)/totalSteps)*100))}%`;
                            stepIndex = (stepIndex + 1) % totalSteps;
                        }, 900);
                        const requestId = result.request_id;
                        let attempts = 0;
                        const maxAttempts = 20;
                        const poll = async () => {
                            try {
                                const r = await fetch(`http://127.0.0.1:8000/api/insights/${requestId}`);
                                const j = await r.json();
                                if (j.status === 'ready') {
                                    clearInterval(stepTimer);
                                    insightsContainer.innerHTML = '';
                                    if (j.insights) appendInsightsBubble(insightsContainer, j.insights);
                                    if (j.follow_up_questions && j.follow_up_questions.length > 0) {
                                        appendFollowUpQuestionsBubble(aiMessageGroup, j.follow_up_questions);
                                        if (avatarEl && avatarEl.parentElement === aiMessageGroup) aiMessageGroup.appendChild(avatarEl);
                                    }
                                    aiMessageGroup.scrollIntoView({ behavior: 'smooth', block: 'start' });
                                    return;
                                } else if (j.status === 'error') {
                                    clearInterval(stepTimer);
                                    insightsContainer.innerHTML = '';
                                    appendMessageBubble(insightsContainer, 'Insights unavailable.');
                                    return;
                                }
                            } catch (_) {}
                            attempts++;
                            if (attempts < maxAttempts) setTimeout(poll, 1000);
                            else {
                                clearInterval(stepTimer);
                                insightsContainer.innerHTML = '';
                                appendMessageBubble(insightsContainer, 'Insights took too long to generate.');
                            }
                        };
                        setTimeout(poll, 1000);
                    }
                    aiMessageGroup.appendChild(tableContainer);
                } else {
                    appendIntroMessageBubble(aiMessageGroup, "Here are the results for your query:");
                    appendTableBubble(aiMessageGroup, result.data);
                }
            } else {
                appendMessageBubble(aiMessageGroup, 'No results found for your query.');
            }

            avatarEl = createAIAvatar();
            aiMessageGroup.appendChild(avatarEl);
            messageArea.appendChild(aiMessageGroup);
            aiMessageGroup.scrollIntoView({ behavior: 'smooth', block: 'start' });
        } catch (error) {
            displayLoading(false);
            setProcessingState(false);
            const aborted = error && (error.name === 'AbortError' || error.code === 20);
            if (aborted) return;
            displayMessage('Sorry, could not connect to the AI service. Please ensure the backend is running.', 'ai');
            console.error('Error sending message:', error);
        }
    }

    // 5. DOM MANIPULATION
    function createAIAvatar() {
        const avatar = document.createElement("div");
        avatar.className = "ai-chat-widget-avatar";
        avatar.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" fill="currentColor" class="bi bi-robot" viewBox="0 0 16 16">
            <path d="M6 12.5a.5.5 0 0 1 .5-.5h3a.5.5 0 0 1 0 1h-3a.5.5 0 0 1-.5-.5M3 8.062C3 6.76 4.235 5.765 5.53 5.886a26.6 26.6 0 0 0 4.94 0C11.765 5.765 13 6.76 13 8.062v1.157a.93.93 0 0 1-.765.935c-.845.147-2.34.346-4.235.346s-3.39-.2-4.235-.346A.93.93 0 0 1 3 9.219zm4.542-.827a.25.25 0 0 0-.217.068l-.92.9a25 25 0 0 1-1.871-.183.25.25 0 0 0-.068.495c.55.076 1.232.149 2.02.193a.25.25 0 0 0 .189-.071l.754-.736.847 1.71a.25.25 0 0 0 .404.062l.932-.97a25 25 0 0 0 1.922-.188.25.25 0 0 0-.068-.495c-.538.074-1.207.145-1.98.189a.25.25 0 0 0-.166.076l-.754.785-.842-1.7a.25.25 0 0 0-.182-.135"/>
            <path d="M8.5 1.866a1 1 0 1 0-1 0V3h-2A4.5 4.5 0 0 0 1 7.5V8a1 1 0 0 0-1 1v2a1 1 0 0 0 1 1v1a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-1a1 1 0 0 0 1-1V9a1 1 0 0 0-1-1v-.5A4.5 4.5 0 0 0 10.5 3h-2zM14 7.5V13a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V7.5A3.5 3.5 0 0 1 5.5 4h5A3.5 3.5 0 0 1 14 7.5"/>
        </svg>`;
        return avatar;
    }

    // This function now handles both user and AI messages, but only creates the container for user messages.
    // For AI messages, the container is managed by the sendMessage function.
    function displayMessage(message, type) {
        if (type === 'user') {
            const container = document.createElement("div");
            container.className = `ai-chat-widget-message-container ${type}`;
            const messageElement = createMessageBubble(message);
            container.appendChild(messageElement);
            messageArea.appendChild(container);
            // Add persistent actions (copy/edit) under user message, and avoid interference with input icons
            ensureUserActions(container);
            container.scrollIntoView({ behavior: 'smooth', block: 'start' });

            // Add user message to conversation history
            conversationHistory.push({
                role: "user",
                content: message
            });
            return container;
        } else {
            // For AI messages, we assume the caller will handle the container and avatar.
            // This is a bit of a simplification; the main logic is now in sendMessage.
            // We'll keep a simple version here for single AI messages like errors.
            const container = document.createElement("div");
            container.className = `ai-chat-widget-message-container ${type}`;
            const messageElement = createMessageBubble(message);
            container.appendChild(messageElement);
            container.appendChild(createAIAvatar());
            messageArea.appendChild(container);
            container.scrollIntoView({ behavior: 'smooth', block: 'start' });
            return container;
        }
    }

    // NEW HELPER: Creates just the message bubble element
    function createMessageBubble(message, additionalClasses = '') {
        const messageElement = document.createElement("div");
        messageElement.className = `ai-chat-widget-message ai ${additionalClasses}`.trim();
        
        // If message is not a string, format as JSON to avoid "[object Object]"
        if (typeof message !== 'string') {
            const pre = document.createElement('pre');
            pre.className = 'ai-chat-widget-code-block';
            const code = document.createElement('code');
            code.textContent = JSON.stringify(message, null, 2);
            pre.appendChild(code);
            messageElement.appendChild(pre);
            return messageElement;
        }

        // Support for markdown-like code blocks
        if (message.includes("```")) {
            const parts = message.split(/```([\s\S]*?)```/);
            let formattedMessage = "";
            
            for (let i = 0; i < parts.length; i++) {
                if (i % 2 === 0) {
                    formattedMessage += parts[i];
                } else {
                    const codeContent = parts[i].trim();
                    const language = codeContent.split("\n")[0].trim();
                    const code = language ? codeContent.substring(language.length).trim() : codeContent;
                    
                    formattedMessage += `<pre class="ai-chat-widget-code-block"><code>${code}</code></pre>`;
                }
            }
            
            messageElement.innerHTML = formattedMessage;
        } else {
            messageElement.innerText = message;
        }
        return messageElement;
    }

    // NEW HELPER: Appends a message bubble to a container
    function appendMessageBubble(container, message) {
        const bubble = createMessageBubble(message);
        container.appendChild(bubble);
    }
    
    // Create intro message bubble with consistent styling
    function appendIntroMessageBubble(container, message) {
        const bubble = createMessageBubble(message, 'intro-message');
        container.appendChild(bubble);
    }
    
    // This function is now just a helper to append a table to a container
    function appendTableBubble(container, data) {
        const messageBubble = createTableBubble(data);
        container.appendChild(messageBubble);
    }

    // NEW FUNCTION: Create and append chart bubble
    function appendChartBubble(container, chartData, tableContainer = null) {
        const chartBubble = createChartBubble(chartData, tableContainer);
        container.appendChild(chartBubble);
        
        // Add special class to container for chart styling
        container.classList.add("ai-chat-widget-chart-message");
    }

    // NEW FUNCTION: Create chart bubble
    function createChartBubble(chartData, tableContainer = null) {
        chartCounter++;
        const chartId = `ai-chat-chart-${chartCounter}`;
        
        // Create the message bubble container
        const messageBubble = document.createElement("div");
        messageBubble.className = "ai-chat-widget-message ai";
        
        // Create chart container with proper sizing to stay within bubble
        const chartContainer = document.createElement("div");
        chartContainer.className = "ai-chat-widget-chart-container";
        
        // Set responsive height based on screen size
        const isMobile = window.innerWidth <= 768;
        chartContainer.style.height = isMobile ? "350px" : "400px"; // Responsive height
        
        // Ensure chart stays within bubble boundaries
        chartContainer.style.width = "100%"; // Full width of available space
        chartContainer.style.maxWidth = "100%"; // Never exceed container
        chartContainer.style.position = "relative";
        chartContainer.style.overflow = "hidden"; // Prevent chart overflow
        chartContainer.style.boxSizing = "border-box"; // Include padding in size calculations
        
        // Create div for Syncfusion chart with proper containment
        const chartDiv = document.createElement("div");
        chartDiv.id = chartId;
        chartDiv.style.width = "100%";
        chartDiv.style.height = "100%";
        chartDiv.style.maxWidth = "100%"; // Ensure chart doesn't exceed container
        chartDiv.style.boxSizing = "border-box";
        chartDiv.style.overflow = "hidden"; // Prevent any chart overflow
        chartContainer.appendChild(chartDiv);
        
        messageBubble.appendChild(chartContainer);
        
        // Add chart controls
        const chartControls = document.createElement("div");
        chartControls.className = "ai-chat-widget-chart-controls";
        
        // Download dropdown
        const downloadDropdown = document.createElement("div");
        downloadDropdown.className = "ai-chat-widget-download-dropdown";
        
        const downloadButton = document.createElement("button");
        downloadButton.className = "ai-chat-widget-download-dropdown-btn";
        downloadButton.innerHTML = '<i class="fas fa-download"></i> Download <span class="dropdown-arrow">▼</span>';
        
        const downloadMenu = document.createElement("div");
        downloadMenu.className = "ai-chat-widget-download-dropdown-menu";
        
        // Create download menu items
        const downloadOptions = [
            { type: 'image', label: 'Image', icon: 'fas fa-image' },
            { type: 'pdf', label: 'PDF', icon: 'fas fa-file-pdf' }
        ];
        
        downloadOptions.forEach(option => {
            const item = document.createElement("div");
            item.className = "ai-chat-widget-download-dropdown-item";
            item.innerHTML = `<i class="${option.icon}"></i> ${option.label}`;
            item.addEventListener("click", () => {
                if (option.type === 'image') {
                    downloadChart(chartId, chartData.title || 'chart');
                } else if (option.type === 'pdf') {
                    downloadChartAsPDF(chartId, chartData.title || 'chart');
                }
                downloadDropdown.classList.remove("open");
            });
            downloadMenu.appendChild(item);
        });
        
        // Toggle dropdown on button click
        downloadButton.addEventListener("click", (e) => {
            e.stopPropagation();
            downloadDropdown.classList.toggle("open");
        });
        
        // Close dropdown when clicking outside
        document.addEventListener("click", (e) => {
            if (!downloadDropdown.contains(e.target)) {
                downloadDropdown.classList.remove("open");
            }
        });
        
        downloadDropdown.appendChild(downloadButton);
        downloadDropdown.appendChild(downloadMenu);
        
        // Fullscreen button
        const fullscreenButton = document.createElement("button");
        fullscreenButton.innerHTML = '<i class="fas fa-expand"></i> Fullscreen';
        fullscreenButton.addEventListener("click", () => viewChartFullscreen(chartId, chartData));
        
        // Change Type button (dropdown)
            const changeTypeDropdown = document.createElement("div");
            changeTypeDropdown.className = "ai-chat-widget-chart-type-dropdown";
            
            const changeTypeButton = document.createElement("button");
            changeTypeButton.className = "ai-chat-widget-chart-type-dropdown-btn";
            changeTypeButton.innerHTML = '<i class="fas fa-chart-bar"></i> Change Type <span class="dropdown-arrow">▼</span>';
            
            const dropdownMenu = document.createElement("div");
            dropdownMenu.className = "ai-chat-widget-chart-type-dropdown-menu";
            
            // Create dropdown menu items for available chart types
            const chartTypes = [
                { type: 'bar', label: 'Bar Chart', icon: 'fas fa-chart-bar' },
                { type: 'column', label: 'Column Chart', icon: 'fas fa-chart-column' },
                { type: 'line', label: 'Line Chart', icon: 'fas fa-chart-line' },
                { type: 'area', label: 'Area Chart', icon: 'fas fa-chart-area' },
                { type: 'pie', label: 'Pie Chart', icon: 'fas fa-chart-pie' },
                { type: 'doughnut', label: 'Doughnut Chart', icon: 'far fa-circle' },
                { type: 'scatter', label: 'Scatter Plot', icon: 'fas fa-braille' },
                { type: 'stackedColumn', label: 'Stacked Column', icon: 'fas fa-layer-group' }
            ];
            
            chartTypes.forEach(chart => {
                const menuItem = document.createElement("div");
                menuItem.className = "ai-chat-widget-chart-type-dropdown-item";
                if (chart.type === chartData.type) {
                    menuItem.classList.add("active");
                }
                menuItem.innerHTML = `<i class="${chart.icon}"></i> ${chart.label}`;
                menuItem.addEventListener("click", () => {
                    // Close dropdown
                    changeTypeDropdown.classList.remove("open");
                    // Change chart type
                    changeChartTypeToSpecific(chartId, chartData, chart.type);
                    // Update active state
                    dropdownMenu.querySelectorAll('.ai-chat-widget-chart-type-dropdown-item').forEach(item => {
                        item.classList.remove('active');
                    });
                    menuItem.classList.add('active');
                });
                dropdownMenu.appendChild(menuItem);
            });
            
            // Toggle dropdown on button click
            changeTypeButton.addEventListener("click", (e) => {
                e.stopPropagation();
                changeTypeDropdown.classList.toggle("open");
            });
            
            // Close dropdown when clicking outside
            document.addEventListener("click", (e) => {
                if (!changeTypeDropdown.contains(e.target)) {
                    changeTypeDropdown.classList.remove("open");
                }
            });
            
            changeTypeDropdown.appendChild(changeTypeButton);
            changeTypeDropdown.appendChild(dropdownMenu);
            chartControls.appendChild(changeTypeDropdown);

        // View Data Table button
        if (tableContainer) {
            const viewDataButton = document.createElement("button");
            viewDataButton.className = "ai-chat-widget-view-data-btn";
            viewDataButton.innerHTML = '<i class="fas fa-eye-slash"></i> Hide Data Table'; // Changed default text with icon
            viewDataButton.addEventListener("click", () => {
                // Find the next sibling (the table container) and toggle its visibility
                const isVisible = tableContainer.style.display !== "none";
                tableContainer.style.display = isVisible ? "none" : "block";
                viewDataButton.innerHTML = isVisible ? '<i class="fas fa-eye"></i> Show Data Table' : '<i class="fas fa-eye-slash"></i> Hide Data Table';
            });
            chartControls.appendChild(viewDataButton);
        }
        
        chartControls.appendChild(downloadDropdown);
        chartControls.appendChild(fullscreenButton);
        messageBubble.appendChild(chartControls);
        
        // Render the chart after a short delay to ensure the DOM is ready
        setTimeout(() => {
            renderChart(chartId, chartData);
        }, 50);
        
        return messageBubble;
    }

    // Helper function to determine if a chart type should use Syncfusion
    function isSyncfusionChartType(type) {
        // Always use Syncfusion for all chart types
        return true;
    }
    
    // Helper function to check if a chart type is enabled
    function isChartTypeEnabled(type) {
        const config = window.CHART_CONFIG;
        if (!config) return true; // If no config, allow all
        
        // Check basic charts
        if (config.enabledCharts && config.enabledCharts.hasOwnProperty(type)) {
            return config.enabledCharts[type];
        }
        
        // Check advanced charts
        if (config.advancedCharts && config.advancedCharts.hasOwnProperty(type)) {
            return config.advancedCharts[type];
        }
        
        // If not explicitly configured, check if it's in our core set
        const coreChartTypes = ['bar', 'column', 'line', 'area', 'pie', 'doughnut', 'scatter', 'stackedColumn', 'kpi', 'combo'];
        return coreChartTypes.includes(type);
    }
    
    // Helper function to get fallback chart type
    function getFallbackChartType(originalType, data) {
        // If original type is enabled, use it
        if (isChartTypeEnabled(originalType)) {
            return originalType;
        }
        
        // Fallback logic based on data characteristics
        if (data && data.labels) {
            const labelCount = data.labels.length;
            
            // For few categories, prefer pie chart
            if (labelCount <= 6) {
                return 'pie';
            }
            
            // For many categories, prefer column chart
            if (labelCount > 10) {
                return 'column';
            }
        }
        
        // Default fallback to bar chart
        return 'bar';
    }
    
    // NEW FUNCTION: Render chart using Syncfusion
    function renderChart(chartId, chartData) {
        // Validate and potentially fallback chart type
        const originalType = chartData.type;
        const validatedType = getFallbackChartType(chartData.type, chartData);
        
        if (validatedType !== originalType) {
            chartData.type = validatedType;
            console.log(`Chart type changed from "${originalType}" to "${validatedType}" for better compatibility`);
        }
        
        // Always use Syncfusion for all chart types
        renderSyncfusionChart(chartId, chartData);
    }

    // Chart.js render function removed - we're now fully using Syncfusion
    
    // NEW FUNCTION: Render chart using Syncfusion with enhanced Zoho Ask Zia-like capabilities
    function renderSyncfusionChart(chartId, chartData) {
        const element = document.getElementById(chartId);
        if (!element) return;
        
        // Handle special chart types first
        if (chartData.type === 'kpi') {
            renderKPIWidget(element, chartData);
            return;
        }
        
        if (chartData.type === 'pivot') {
            renderPivotView(element, chartData);
            return;
        }
        
        if (chartData.type.startsWith('map_')) {
            renderMapChart(element, chartData);
            return;
        }
        
        // Always use the default 'Material' theme
        const theme = 'Material';
        
        // Get explicit color palette from config - only these colors should be used
        // Pull palette and randomize order per chart render for visual variety
        const basePalette = (window.CHART_CONFIG && window.CHART_CONFIG.palette) || [
            '#fe6383', '#36a2eb', '#ffcc55', '#4ac0c0', '#ff9f40', '#9966ff', '#42b982', '#cacbce'
        ];
        const palette = [...basePalette].sort(() => Math.random() - 0.5);
        
        // Common chart options
        const chartOptions = {
            title: chartData.title || '',
            theme: theme,
            palette: palette
        };
        
        // Prepare data for Syncfusion charts
        const chartType = mapChartTypeToSyncfusion(chartData.type);
        let series = [];
        let chartInstance = null;
        
        // Different chart types need different data format
        switch (chartType) {
            case 'Pie':
            case 'Doughnut':
            case 'Funnel':
            case 'Pyramid':
                // For single-series charts like pie/doughnut
                const dataset = chartData.datasets[0];
                // For these charts, data needs to be in format: [{x: 'Label', y: value}, ...]
                const data = chartData.labels.map((label, index) => ({
                    x: label,
                    y: dataset.data[index],
                    text: `${label}: ${dataset.data[index]}` , // For tooltip/dataLabel
                    fill: chartOptions.palette[index % chartOptions.palette.length] // enforce custom color
                }));
                
                series = [{
                    dataSource: data,
                    name: dataset.label || '',
                    xName: 'x',
                    yName: 'y',
                    pointColorMapping: 'fill',
                    innerRadius: chartType === 'Doughnut' ? '40%' : '0%',
                    dataLabel: {
                        visible: false,
                        position: 'Inside',
                        name: 'text',
                        font: { fontWeight: '600' }
            }
                }];
        
                // Create the appropriate chart
                if (chartType === 'Pie' || chartType === 'Doughnut') {
                    chartInstance = new ej.charts.AccumulationChart({
                        title: chartOptions.title,
                        series: series,
                        legendSettings: { visible: true, position: 'Right' },
                        enableSmartLabels: true,
                        tooltip: { enable: true },
                        theme: chartOptions.theme
                    });
                } else if (chartType === 'Funnel') {
                    chartInstance = new ej.charts.AccumulationChart({
                        title: chartOptions.title,
                        series: series,
                        legendSettings: { visible: true, position: 'Right' },
                        enableSmartLabels: true,
                        tooltip: { enable: true },
                        theme: chartOptions.theme
                    });
                    // Apply funnel series type
                    chartInstance.series[0].type = 'Funnel';
                } else if (chartType === 'Pyramid') {
                    chartInstance = new ej.charts.AccumulationChart({
                        title: chartOptions.title,
                        series: series,
                        legendSettings: { visible: true, position: 'Right' },
                        enableSmartLabels: true,
                        tooltip: { enable: true },
                        theme: chartOptions.theme
                    });
                    // Apply pyramid series type
                    chartInstance.series[0].type = 'Pyramid';
                }
                break;
                
            case 'Scatter':
            case 'Bubble':
                // Format data for scatter/bubble charts
                series = chartData.datasets.map(dataset => {
                    // For bubble charts we need a size value
                    const bubbleData = chartData.labels.map((label, index) => {
                        const point = {
                            x: index, // Use index as x value
                            y: dataset.data[index],
                            text: label
                        };
                        
                        // For bubble chart, add size - if not provided, use a default based on value
                        if (chartType === 'Bubble') {
                            point.size = dataset.bubbleSizes ? 
                                dataset.bubbleSizes[index] : 
                                Math.max(Math.abs(dataset.data[index]) * 0.2, 5);
                        }
                        
                        return point;
                    });
                    
                    return {
                        dataSource: bubbleData,
                        name: dataset.label || '',
                        xName: 'x',
                        yName: 'y',
                        size: chartType === 'Bubble' ? 'size' : undefined,
                        type: chartType,
                        marker: {
                            visible: true,
                            height: 10,
                            width: 10
                        }
                    };
                });
        
        // Create the chart
                chartInstance = new ej.charts.Chart({
                    title: chartOptions.title,
                    primaryXAxis: {
                        valueType: 'Category',
                        title: 'Categories',
                        labelFormat: '{value}',
                        labelPlacement: 'OnTicks'
                    },
                    primaryYAxis: {
                        title: 'Values',
                        labelFormat: '{value}'
                    },
                    series: series,
                    tooltip: { enable: true },
                    legendSettings: { visible: true },
                    theme: chartOptions.theme,
                    palettes: chartOptions.palette
                });
                break;
                
            case 'Radar':
            case 'Polar':
                // Format data for radar/polar charts
                series = chartData.datasets.map(dataset => {
                    return {
                        dataSource: chartData.labels.map((label, index) => ({
                            x: label,
                            y: dataset.data[index]
                        })),
                        name: dataset.label || '',
                        xName: 'x',
                        yName: 'y',
                        type: chartType === 'Radar' ? 'Radar' : 'Polar',
                        drawType: chartType === 'Radar' ? 'Line' : 'Column',
                        marker: {
                            visible: true
                        }
                    };
                });
        
        // Create the chart
                chartInstance = new ej.charts.Chart({
                    title: chartOptions.title,
                    primaryXAxis: {
                        valueType: 'Category',
                        labelPlacement: 'OnTicks',
                        interval: 1
                    },
                    series: series,
                    tooltip: { enable: true },
                    legendSettings: { visible: true },
                    theme: chartOptions.theme,
                    palettes: chartOptions.palette
                });
                break;
                
            default: // Bar, Line, Area, Column, etc.
                // For multi-series charts
                series = chartData.datasets.map(dataset => {
                    return {
                        dataSource: chartData.labels.map((label, index) => ({
                            x: label,
                            y: dataset.data[index]
                        })),
                        name: dataset.label || '',
                        xName: 'x',
                        yName: 'y',
                        type: chartType,
                        marker: chartType === 'Line' || chartType === 'Area' || chartType === 'Spline' ? {
                            visible: true
                        } : undefined
                        // Let palette handle colors instead of individual fill properties
                    };
                });
                
                // Create the chart
                chartInstance = new ej.charts.Chart({
                    title: chartOptions.title,
                    primaryXAxis: {
                        valueType: 'Category',
                        title: 'Categories',
                        labelFormat: '{value}',
                        labelRotation: chartData.labels.length > 10 ? 45 : 0 // Rotate labels if many
                    },
                    primaryYAxis: {
                        title: 'Values',
                        labelFormat: '{value}'
                    },
                    series: series,
                    tooltip: { enable: true },
                    legendSettings: { visible: true },
                    theme: chartOptions.theme,
                    palettes: chartOptions.palette
                });
        }
        
        // Render the chart
        if (chartInstance) {
            chartInstance.appendTo(`#${chartId}`);
        
            // Store the chart instance for later reference
            element.ejChart = chartInstance;
        }
    }
    
    // Helper function to map Chart.js chart types to Syncfusion chart types
    function mapChartTypeToSyncfusion(chartJsType) {
        const typeMap = {
            // Core supported chart types
            'bar': 'Bar',
            'horizontalBar': 'Bar',
            'line': 'Line',
            'pie': 'Pie',
            'doughnut': 'Doughnut',
            'area': 'Area',
            'scatter': 'Scatter',
            'column': 'Column',
            'stackedColumn': 'StackedColumn',
            
            // Fallbacks for disabled chart types
            'spline': 'Line',        // Fallback to line chart
            'bubble': 'Scatter',     // Fallback to scatter plot
            'stacked': 'StackedColumn',
            'stackedBar': 'StackedColumn', // Fallback to stacked column
            'radar': 'Column',       // Fallback to column chart
            'polar': 'Column',       // Fallback to column chart
            'funnel': 'Bar',         // Fallback to bar chart
            'pyramid': 'Bar',        // Fallback to bar chart
            'heatmap': 'Column',     // Fallback to column chart
            'boxplot': 'Column',     // Fallback to column chart
            'waterfall': 'Column'    // Fallback to column chart
        };
        
        return typeMap[chartJsType] || 'Column'; // Default to Column if not found
    }

    // Function to download chart as image
    function downloadChart(chartId, title) {
        const element = document.getElementById(chartId);
        if (!element || !element.ejChart) return;
        
        // For Syncfusion charts
        element.ejChart.export('PNG', title || 'chart');
    }

    // Function to download chart as PDF
    function downloadChartAsPDF(chartId, title) {
        const element = document.getElementById(chartId);
        if (!element || !element.ejChart) return;
        
        // For Syncfusion charts
        element.ejChart.export('PDF', title || 'chart');
    }

    // NEW FUNCTION: View chart in fullscreen
    function viewChartFullscreen(chartId, chartData) {
        const originalElement = document.getElementById(chartId);
        if (!originalElement) return;
        
        // Create modal container
        const modal = document.createElement("div");
        modal.className = "ai-chat-widget-modal";
        
        const modalContent = document.createElement("div");
        modalContent.className = "ai-chat-widget-modal-content";
        modalContent.style.width = "90%";
        modalContent.style.height = "90%";
        modalContent.style.maxWidth = "1000px";
        
        // Close button
        const closeBtn = document.createElement("span");
        closeBtn.className = "ai-chat-widget-modal-close";
        closeBtn.innerHTML = "&times;";
        closeBtn.addEventListener("click", () => {
            document.body.removeChild(modal);
        });
        
        // Create container for the new chart with proper containment
        const chartContainer = document.createElement("div");
        chartContainer.style.width = "100%";
        chartContainer.style.height = "90%";
        chartContainer.style.position = "relative";
        chartContainer.style.boxSizing = "border-box";
        chartContainer.style.overflow = "hidden"; // Prevent overflow in fullscreen
        
        // Create element for the fullscreen chart
        const fullscreenChartId = `fullscreen-${chartId}`;
        
        // Create div for Syncfusion chart with proper containment
        const chartDiv = document.createElement("div");
        chartDiv.id = fullscreenChartId;
        chartDiv.style.width = "100%";
        chartDiv.style.height = "100%";
        chartDiv.style.maxWidth = "100%";
        chartDiv.style.boxSizing = "border-box";
        chartDiv.style.overflow = "hidden";
        chartDiv.style.height = "100%";
        chartContainer.appendChild(chartDiv);
        
        modalContent.appendChild(closeBtn);
        modalContent.appendChild(chartContainer);
        modal.appendChild(modalContent);
        
        document.body.appendChild(modal);
        
        // Render the chart in the modal
        setTimeout(() => {
            renderChart(fullscreenChartId, chartData);
        }, 50);
        
        // Close modal when clicking outside content
        modal.addEventListener("click", (e) => {
            if (e.target === modal) {
                document.body.removeChild(modal);
            }
        });
    }

    // NEW FUNCTION: Change chart type
    function changeChartType(chartId, chartData) {
        const element = document.getElementById(chartId);
        if (!element) return;
        
        // Define the chart type cycle for core chart types only
        // Simplified chart types for improved accuracy
        const chartTypes = ['bar', 'column', 'line', 'area', 'pie', 'doughnut', 'scatter', 'stackedColumn'];
        
        // Get current type and find the next in cycle
        const currentType = chartData.type;
        let currentIndex = chartTypes.indexOf(currentType);
        if (currentIndex === -1) currentIndex = 0; // Default to first if not found
        const nextIndex = (currentIndex + 1) % chartTypes.length;
        const newType = chartTypes[nextIndex];
        
        // Update chart data type
        chartData.type = newType;
        
        // Destroy current chart and create new one
        if (element.ejChart) {
            // For Syncfusion charts
            element.ejChart.destroy();
        } else if (element.chart) {
            // Handle any existing Chart.js charts (for backward compatibility)
            element.chart.destroy();
                }
        
        element.innerHTML = '';
        
        // Re-render with new chart type using Syncfusion
        renderSyncfusionChart(chartId, chartData);
        
        // Check if this chart is currently displayed in fullscreen and update it too
        const fullscreenId = `fullscreen-${chartId}`;
        const fullscreenElement = document.getElementById(fullscreenId);
        if (fullscreenElement) {
            if (fullscreenElement.ejChart) {
                // For Syncfusion charts
                fullscreenElement.ejChart.destroy();
            } else if (fullscreenElement.chart) {
                // Handle any existing Chart.js charts
                fullscreenElement.chart.destroy();
            }
            
            fullscreenElement.innerHTML = '';
            renderSyncfusionChart(fullscreenId, chartData);
        }
    }

    function changeChartTypeToSpecific(chartId, chartData, newType) {
        const element = document.getElementById(chartId);
        if (!element) return;
        
        // Update chart data type to the specific type
        chartData.type = newType;
        
        // Destroy current chart and create new one
        if (element.ejChart) {
            // For Syncfusion charts
            element.ejChart.destroy();
        } else if (element.chart) {
            // Handle any existing Chart.js charts (for backward compatibility)
            element.chart.destroy();
        }
        
        element.innerHTML = '';
        
        // Re-render with new chart type using Syncfusion
        renderSyncfusionChart(chartId, chartData);
        
        // Check if this chart is currently displayed in fullscreen and update it too
        const fullscreenId = `fullscreen-${chartId}`;
        const fullscreenElement = document.getElementById(fullscreenId);
        if (fullscreenElement) {
            if (fullscreenElement.ejChart) {
                // For Syncfusion charts
                fullscreenElement.ejChart.destroy();
            } else if (fullscreenElement.chart) {
                // Handle any existing Chart.js charts
                fullscreenElement.chart.destroy();
            }
            
            fullscreenElement.innerHTML = '';
            renderSyncfusionChart(fullscreenId, chartData);
        }
    }

    // NEW FUNCTION: Create and append insights bubble
    function appendInsightsBubble(container, insights) {
        const insightsBubble = createInsightsBubble(insights);
        container.appendChild(insightsBubble);
    }

    // NEW FUNCTION: Create insights bubble
    function createInsightsBubble(insights) {
        const messageBubble = document.createElement("div");
        messageBubble.className = "ai-chat-widget-message ai ai-chat-insights";
        
        // Add insights icon and content
        const insightsHeader = document.createElement("div");
        insightsHeader.className = "ai-chat-insights-header";
        insightsHeader.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" style="margin-right: 8px;">
                <path d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"/>
            </svg>
            <strong>Key Insights</strong>
        `;
        
        const insightsContent = document.createElement("div");
        insightsContent.className = "ai-chat-insights-content";
        insightsContent.textContent = insights;
        
        messageBubble.appendChild(insightsHeader);
        messageBubble.appendChild(insightsContent);
        
        return messageBubble;
    }

    // NEW FUNCTION: Create and append follow-up questions bubble
    function appendFollowUpQuestionsBubble(container, questions) {
        const followUpBubble = createFollowUpQuestionsBubble(questions);
        container.appendChild(followUpBubble);
    }

    // NEW FUNCTION: Create follow-up questions bubble
    function createFollowUpQuestionsBubble(questions) {
        const messageBubble = document.createElement("div");
        messageBubble.className = "ai-chat-widget-message ai ai-chat-followup";
        
        // Add follow-up header
        const followUpHeader = document.createElement("div");
        followUpHeader.className = "ai-chat-followup-header";
        followUpHeader.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" style="margin-right: 8px;">
                <path d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
            </svg>
            <strong>Related Questions</strong>
        `;
        
        const followUpContent = document.createElement("div");
        followUpContent.className = "ai-chat-followup-content";
        
        questions.forEach((question, index) => {
            const questionItem = document.createElement("div");
            questionItem.className = "ai-chat-followup-question";
            questionItem.innerHTML = `
                <span class="ai-chat-followup-number">${index + 1}.</span>
                <span class="ai-chat-followup-text">${question}</span>
                <button class="ai-chat-followup-btn" data-question="${question.replace(/"/g, '&quot;').replace(/'/g, "&#39;")}">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M12 4l-1.41 1.41L16.17 11H4v2h12.17l-5.58 5.59L12 20l8-8z"/>
                    </svg>
                </button>
            `;
            // Add click handler to arrow button to place question in input
            const arrowBtn = questionItem.querySelector('.ai-chat-followup-btn');
            if (arrowBtn) {
                arrowBtn.addEventListener('click', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    const q = arrowBtn.getAttribute('data-question');
                    if (inputField && q) {
                        inputField.value = q;
                        inputField.focus();
                    }
                });
            }
            followUpContent.appendChild(questionItem);
        });
        
        messageBubble.appendChild(followUpHeader);
        messageBubble.appendChild(followUpContent);
        
        return messageBubble;
    }

    // NEW FUNCTION: Set follow-up query in input field
    function setFollowUpQuery(question) {
        if (inputField) {
            inputField.value = question;
            inputField.focus();
        }
    }

    function createTableBubble(data) {
        // This new container holds the message bubble
        const messageBubble = document.createElement("div");
        messageBubble.className = "ai-chat-widget-message ai";
    
        const tableWrapper = document.createElement("div");
        tableWrapper.className = "ai-chat-widget-table-wrapper";
        
        const table = document.createElement("table");
        const thead = document.createElement("thead");
        const tbody = document.createElement("tbody");
    
        // Create header row
        const headerRow = document.createElement("tr");
        const srNoHeader = document.createElement("th");
        srNoHeader.innerText = "Sr. No.";
        headerRow.appendChild(srNoHeader);

        const headers = Object.keys(data[0]);
        headers.forEach(key => {
            const th = document.createElement("th");
            th.innerText = key;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
    
        // Create body rows
        data.forEach((rowData, index) => {
            const row = document.createElement("tr");
            
            const srNoCell = document.createElement("td");
            srNoCell.innerText = index + 1;
            row.appendChild(srNoCell);

            Object.values(rowData).forEach(value => {
                const td = document.createElement("td");
                td.innerText = value;
                row.appendChild(td);
            });
            tbody.appendChild(row);
        });
    
        table.appendChild(thead);
        table.appendChild(tbody);
        tableWrapper.appendChild(table);
        messageBubble.appendChild(tableWrapper);
        
        // Add table controls
        const tableControls = document.createElement("div");
        tableControls.className = "ai-chat-widget-table-controls";
        
        const downloadButton = document.createElement("button");
        downloadButton.innerHTML = '<i class="fas fa-download"></i> CSV';
        downloadButton.addEventListener("click", () => downloadCSV(data, headers));
        
        const fullscreenButton = document.createElement("button");
        fullscreenButton.innerHTML = '<i class="fas fa-expand"></i> Fullscreen';
        fullscreenButton.addEventListener("click", () => viewFullscreen(table.cloneNode(true)));
        
        tableControls.appendChild(downloadButton);
        tableControls.appendChild(fullscreenButton);
        messageBubble.appendChild(tableControls);
        
        return messageBubble;
    }

    function displayTable(data) {
        const container = document.createElement("div");
        container.className = "ai-chat-widget-message-container ai";

        const messageBubble = createTableBubble(data);
        
        // Add the message bubble to the main container
        container.appendChild(messageBubble);
        
        // Add the AI avatar
        container.appendChild(createAIAvatar());
        
        messageArea.appendChild(container);
        // Scroll to the top of the new table message
        container.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    function downloadCSV(data, headers) {
        let csvContent = "data:text/csv;charset=utf-8,";
        
        // Add headers
        csvContent += headers.join(",") + "\r\n";
        
        // Add rows
        data.forEach(function(rowData) {
            const row = headers.map(header => {
                const cell = rowData[header] || "";
                // Escape quotes and wrap in quotes if contains comma
                return typeof cell === 'string' && (cell.includes(',') || cell.includes('"')) 
                    ? `"${cell.replace(/"/g, '""')}"` 
                    : cell;
            });
            csvContent += row.join(",") + "\r\n";
        });
        
        const encodedUri = encodeURI(csvContent);
        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", "data_export.csv");
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    function viewFullscreen(tableElement) {
        const modal = document.createElement("div");
        modal.className = "ai-chat-widget-modal";
        
        const modalContent = document.createElement("div");
        modalContent.className = "ai-chat-widget-modal-content";
        
        const closeBtn = document.createElement("span");
        closeBtn.className = "ai-chat-widget-modal-close";
        closeBtn.innerHTML = "&times;";
        closeBtn.addEventListener("click", () => {
            document.body.removeChild(modal);
        });
        
        modalContent.appendChild(closeBtn);
        modalContent.appendChild(tableElement);
        modal.appendChild(modalContent);
        
        document.body.appendChild(modal);
        
        // Close modal when clicking outside content
        modal.addEventListener("click", (e) => {
            if (e.target === modal) {
                document.body.removeChild(modal);
            }
        });
    }

    function displayLoading(isLoading) {
        const existingLoading = document.querySelector(".ai-chat-widget-loading");
        if (existingLoading) {
            existingLoading.remove();
        }
        
        if (isLoading) {
            const container = document.createElement("div");
            container.className = "ai-chat-widget-message-container ai ai-chat-widget-loading";
            
            const loadingElement = document.createElement("div");
            loadingElement.className = "ai-chat-widget-message ai";
            
            const dots = document.createElement("div");
            dots.className = "ai-chat-widget-loading-dots";
            dots.innerHTML = `
                <div class="ai-chat-widget-loading-dot"></div>
                <div class="ai-chat-widget-loading-dot"></div>
                <div class="ai-chat-widget-loading-dot"></div>
            `;
            
            loadingElement.appendChild(dots);
            container.appendChild(loadingElement);
            container.appendChild(createAIAvatar());
            
            messageArea.appendChild(container);
            messageArea.scrollTop = messageArea.scrollHeight;
        }
    }

    // 6. INITIALIZATION
    document.addEventListener("DOMContentLoaded", function() {
        initChatWidget();
        addEventListeners();
    });
})();