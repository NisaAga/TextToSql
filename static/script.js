// static/script.js

document.addEventListener('DOMContentLoaded', () => {
    // 1. Initial Call to get the provider name immediately when the page loads
    fetchProviderName();
});

// Function to fetch the provider name from the server and display it
async function fetchProviderName() {
    try {
        // We assume Flask has the /api/provider_name endpoint
        const response = await fetch('/api/provider_name');

        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }

        const data = await response.json();
        document.getElementById('provider-name').textContent = data.provider;
    } catch (error) {
        console.error('Error fetching provider name:', error);
        document.getElementById('provider-name').textContent = 'Error loading provider';
    }
}

// Main function called by the HTML button's onclick="sendQuery()"
async function sendQuery() {
    const nlQueryInput = document.getElementById('nl_query');
    const sqlOutput = document.getElementById('sql_output');
    const resultsTable = document.getElementById('results_table');
    const providerElement = document.getElementById('provider-name');

    const naturalLanguageQuery = nlQueryInput.value.trim();

    if (!naturalLanguageQuery) {
        alert("Please enter a natural language question.");
        return;
    }

    // --- Update UI for Loading State ---
    sqlOutput.textContent = 'Generating SQL...';
    resultsTable.innerHTML = `<thead><tr><th colspan="100%">Executing query...</th></tr></thead><tbody></tbody>`;

    try {
        // 1. Send the POST request to the Flask API endpoint
        const response = await fetch('/api/query', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            // Send the query in the request body as JSON
            body: JSON.stringify({ query: naturalLanguageQuery }),
        });

        // Check for HTTP errors
        if (!response.ok) {
            // If the server responded with a non-200 status (e.g., 400, 500)
            const errorText = await response.text();
            throw new Error(`Server returned status ${response.status}: ${errorText.substring(0, 100)}...`);
        }

        // 2. Parse the JSON response
        const data = await response.json();

        // 3. Update the HTML elements
        sqlOutput.textContent = data.generated_sql || 'Error: No SQL generated';
        if (data.provider) {
             providerElement.textContent = data.provider;
        }

        // Clear previous results
        resultsTable.innerHTML = '';

        // --- Handle Backend Errors or Display Results ---

        // Check if the backend returned an internal error state
        if (data.headers && data.headers.includes("Error")) {
             resultsTable.innerHTML = `
                <thead><tr><th>API/SQL Execution Error</th></tr></thead>
                <tbody><tr><td>${data.generated_sql}</td></tr></tbody>
            `;
        }
        // Display Results
        else if (data.results && data.results.length > 0) {
            // Create Table Header
            let headerHtml = '<thead><tr>';
            data.headers.forEach(header => {
                headerHtml += `<th>${header}</th>`;
            });
            headerHtml += '</tr></thead>';

            // Create Table Body Rows
            let bodyHtml = '<tbody>';
            data.results.forEach(row => {
                bodyHtml += '<tr>';
                // Each cell is an element in the row array
                row.forEach(cell => {
                    // Use a simple check for null/undefined and convert to string
                    const displayCell = cell === null || cell === undefined ? 'NULL' : String(cell);
                    bodyHtml += `<td>${displayCell}</td>`;
                });
                bodyHtml += '</tr>';
            });
            bodyHtml += '</tbody>';

            resultsTable.innerHTML = headerHtml + bodyHtml;
        } else {
            // Case for successful query but zero results (e.g., SELECT that returns no rows)
            resultsTable.innerHTML = `
                <thead><tr><th>Info</th></tr></thead>
                <tbody><tr><td>Query executed successfully, but returned no results.</td></tr></tbody>
            `;
        }

    } catch (error) {
        console.error('Fetch error:', error);
        sqlOutput.textContent = 'Failed to communicate with the server.';
        resultsTable.innerHTML = `
            <thead><tr><th style="color: red;">Connection Error</th></tr></thead>
            <tbody><tr><td>${error.message}. Please ensure the Python server is running and accessible.</td></tr></tbody>
        `;
    }
}