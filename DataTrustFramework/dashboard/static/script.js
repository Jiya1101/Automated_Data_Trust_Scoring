const dropzone = document.getElementById('dropzone');
const fileInput = document.getElementById('fileInput');
const fileInfo = document.getElementById('fileInfo');
const fileNameDisplay = document.getElementById('fileName');
const clearBtn = document.getElementById('clearBtn');
const analyzeBtn = document.getElementById('analyzeBtn');
const loadingState = document.getElementById('loadingState');

// Score Elements
const scoreCircle = document.getElementById('scoreCircle');
const scoreText = document.getElementById('scoreText');
const scoreStatus = document.getElementById('scoreStatus');

// Metric Elements
const metTotal = document.getElementById('totalRecords');
const metComp = document.getElementById('metaCompleteness');
const metAnom = document.getElementById('metaAnomaly');

let radarChart;
let selectedFile = null;

// Initialize Chart
function initChart() {
    const ctx = document.getElementById('qualityChart').getContext('2d');
    
    Chart.defaults.color = '#64748b';
    Chart.defaults.font.family = "'Playfair Display', serif";
    
    radarChart = new Chart(ctx, {
        type: 'radar',
        data: {
            labels: ['Completeness', 'Accuracy', 'Reputation', 'Trust'],
            datasets: [{
                label: 'Score Matrix',
                data: [0, 0, 0, 0],
                backgroundColor: 'rgba(15, 23, 42, 0.1)',
                borderColor: '#0f172a',
                pointBackgroundColor: '#0f172a',
                pointBorderColor: '#fff',
                pointHoverBackgroundColor: '#fff',
                pointHoverBorderColor: '#0f172a',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                r: {
                    angleLines: { color: 'rgba(255, 255, 255, 0.1)' },
                    grid: { color: 'rgba(255, 255, 255, 0.1)' },
                    pointLabels: {
                        color: '#fff',
                        font: { size: 11, weight: '600' }
                    },
                    ticks: { display: false, min: 0, max: 100 }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });
}
initChart();

// Update UI methods
function updateScore(score) {
    const dashVal = `${score}, 100`;
    scoreCircle.style.strokeDasharray = dashVal;
    
    let color = '#ef4444'; // Danger
    let status = 'Critical Risk';
    
    if (score >= 80) { color = '#10b981'; status = 'High Trust'; }
    else if (score >= 50) { color = '#f59e0b'; status = 'Moderate Trust'; }
    
    scoreCircle.style.stroke = color;
    scoreText.textContent = score;
    scoreStatus.textContent = status;
    scoreStatus.style.color = color;
}

function updateMetrics(metrics) {
    metTotal.textContent = metrics.total_records.toLocaleString();
    metComp.textContent = metrics.completeness + '%';
    metAnom.textContent = metrics.anomaly_rate + '%';
    
    // Update Danger threshold for Anomaly UI
    document.querySelector('.metric-card.alert').style.borderColor = 
        metrics.anomaly_rate > 5 ? 'rgba(239, 68, 68, 0.5)' : '#e2e8f0';
        
    radarChart.data.datasets[0].data = [
        metrics.completeness,
        metrics.accuracy,
        metrics.reputation,
        metrics.trust_score
    ];
    radarChart.update();
}

// Drag functionality
dropzone.addEventListener('click', () => fileInput.click());

['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
    dropzone.addEventListener(eventName, preventDefaults, false);
});
function preventDefaults(e) {
    e.preventDefault();
    e.stopPropagation();
}

['dragenter', 'dragover'].forEach(eventName => {
    dropzone.addEventListener(eventName, () => dropzone.classList.add('dragover'), false);
});
['dragleave', 'drop'].forEach(eventName => {
    dropzone.addEventListener(eventName, () => dropzone.classList.remove('dragover'), false);
});

dropzone.addEventListener('drop', handleDrop, false);
fileInput.addEventListener('change', (e) => handleFiles(e.target.files));

function handleDrop(e) {
    handleFiles(e.dataTransfer.files);
}

function handleFiles(files) {
    if (files.length > 0) {
        selectedFile = files[0];
        if (selectedFile.name.endsWith('.csv')) {
            fileNameDisplay.textContent = selectedFile.name;
            dropzone.classList.add('hidden');
            fileInfo.classList.remove('hidden');
        } else {
            alert('Please upload a CSV file.');
        }
    }
}

clearBtn.addEventListener('click', () => {
    selectedFile = null;
    fileInput.value = '';
    fileInfo.classList.add('hidden');
    dropzone.classList.remove('hidden');
});

// Output the parsed Anomalies directly
function updateAnomaliesTable(anomalies) {
    const tableSection = document.getElementById('anomaliesSection');
    const tbody = document.getElementById('anomalyTableBody');
    tbody.innerHTML = ''; // clear
    
    if (!anomalies || anomalies.length === 0) {
        tableSection.classList.add('hidden');
        return;
    }
    
    tableSection.classList.remove('hidden');
    
    anomalies.forEach(anomaly => {
        const severityStr = parseFloat(anomaly.anomaly_severity).toFixed(2);
        const trustStr = parseFloat(anomaly.trust_score).toFixed(2);
        let severityClass = 'severity-medium';
        if (anomaly.anomaly_severity > 0.8) severityClass = 'severity-high';
        
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${anomaly.user_id}</td>
            <td><strong>${anomaly.source_id}</strong></td>
            <td>$${parseFloat(anomaly.amount).toFixed(2)}</td>
            <td class="${severityClass}">${severityStr}</td>
            <td>${trustStr}</td>
        `;
        tbody.appendChild(row);
    });
}

// Upload and Analyze
analyzeBtn.addEventListener('click', async () => {
    if (!selectedFile) return;
    
    fileInfo.classList.add('hidden');
    loadingState.classList.remove('hidden');
    
    const formData = new FormData();
    formData.append('file', selectedFile);
    
    try {
        const res = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });
        const data = await res.json();
        
        if (data.status === 'success') {
            updateScore(data.metrics.trust_score);
            updateMetrics(data.metrics);
            if (data.metrics.top_10_anomalies) {
                updateAnomaliesTable(data.metrics.top_10_anomalies);
            }
        } else {
            alert(`Analysis failed: ${data.message}`);
        }
    } catch (err) {
        alert('Network error occurred during analysis.');
        console.error(err);
    } finally {
        loadingState.classList.add('hidden');
        fileInfo.classList.remove('hidden');
    }
});
