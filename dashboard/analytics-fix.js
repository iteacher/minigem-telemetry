// Fixed advanced analytics charts
function renderAdvancedAnalytics(data) {
  console.log('Rendering advanced analytics with data:', data);
  
  // Add safety checks for all analytics data
  if (!data) {
    console.log('No analytics data provided');
    return;
  }
  
  // Seasonal Patterns Chart - Fixed
  if (data.seasonalPatterns && Array.isArray(data.seasonalPatterns) && data.seasonalPatterns.length > 0) {
    console.log('Seasonal patterns data:', data.seasonalPatterns);
    try {
      const maxValue = Math.max(...data.seasonalPatterns.map(p => p.average || 0));
      
      createOrUpdateChart('#seasonalChart', {
        type: 'bar',
        data: {
          labels: data.seasonalPatterns.map(p => p.month),
          datasets: [{
            label: 'Average Installs',
            data: data.seasonalPatterns.map(p => p.average || 0),
            backgroundColor: data.seasonalPatterns.map(p => {
              const intensity = maxValue > 0 ? (p.average || 0) / maxValue : 0;
              return `rgba(59, 130, 246, ${0.3 + intensity * 0.7})`;
            }),
            borderColor: '#3b82f6',
            borderWidth: 1
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false }
          },
          scales: {
            y: {
              beginAtZero: true,
              ticks: { color: '#9ca3af' },
              grid: { color: '#374151' }
            },
            x: {
              ticks: { color: '#9ca3af' },
              grid: { display: false }
            }
          }
        }
      });
    } catch (error) {
      console.error('Error rendering seasonal chart:', error);
    }
  } else {
    console.log('No seasonal patterns data available');
  }

  // Geographic Growth Chart - Fixed (NO horizontalBar)
  if (data.geographicGrowth && typeof data.geographicGrowth === 'object' && Object.keys(data.geographicGrowth).length > 0) {
    console.log('Geographic growth data:', data.geographicGrowth);
    try {
      const countries = Object.keys(data.geographicGrowth).slice(0, 10);
      const growthRates = countries.map(c => data.geographicGrowth[c]?.growthRate || 0);
      
      createOrUpdateChart('#geoGrowthChart', {
        type: 'bar', // FIXED: Use 'bar' not 'horizontalBar'
        data: {
          labels: countries,
          datasets: [{
            label: 'Growth Rate (%)',
            data: growthRates,
            backgroundColor: growthRates.map(rate => 
              rate > 20 ? '#10b981' : rate > 5 ? '#3b82f6' : rate > 0 ? '#f59e0b' : '#ef4444'
            ),
            borderWidth: 0
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          indexAxis: 'y', // This makes it horizontal
          plugins: {
            legend: { display: false }
          },
          scales: {
            y: {
              ticks: { color: '#9ca3af' },
              grid: { display: false }
            },
            x: {
              beginAtZero: true,
              ticks: { color: '#9ca3af' },
              grid: { color: '#374151' }
            }
          }
        }
      });
    } catch (error) {
      console.error('Error rendering geographic growth chart:', error);
    }
  } else {
    console.log('No geographic growth data available');
  }

  // Version Adoption Speed - Fixed
  if (data.versionMigration && typeof data.versionMigration === 'object') {
    console.log('Version migration data:', data.versionMigration);
    try {
      const versions = Object.keys(data.versionMigration).slice(-10);
      const adoptionSpeeds = versions.map(v => data.versionMigration[v]?.adoptionMonths || 0).filter(s => s > 0);
      const versionLabels = versions.filter(v => (data.versionMigration[v]?.adoptionMonths || 0) > 0);
      
      if (adoptionSpeeds.length > 0) {
        createOrUpdateChart('#adoptionChart', {
          type: 'line',
          data: {
            labels: versionLabels,
            datasets: [{
              label: 'Months to Adoption',
              data: adoptionSpeeds,
              borderColor: '#8b5cf6',
              backgroundColor: '#8b5cf6',
              borderWidth: 2,
              fill: false,
              tension: 0.3,
              pointRadius: 4
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              y: {
                beginAtZero: true,
                title: {
                  display: true,
                  text: 'Months to 80% Adoption',
                  color: '#9ca3af'
                },
                ticks: { color: '#9ca3af' },
                grid: { color: '#374151' }
              },
              x: {
                ticks: { color: '#9ca3af' },
                grid: { display: false }
              }
            }
          }
        });
      }
    } catch (error) {
      console.error('Error rendering adoption chart:', error);
    }
  }

  // Platform Trends - Fixed
  if (data.platformTrends && typeof data.platformTrends === 'object') {
    console.log('Platform trends data:', data.platformTrends);
    try {
      const platforms = Object.keys(data.platformTrends).slice(0, 8);
      const validPlatforms = platforms.filter(p => data.platformTrends[p] && typeof data.platformTrends[p] === 'object');
      
      if (validPlatforms.length > 0) {
        createOrUpdateChart('#platformTrendsChart', {
          type: 'scatter',
          data: {
            datasets: [{
              label: 'Platform Performance',
              data: validPlatforms.map((platform) => ({
                x: data.platformTrends[platform]?.marketShare || 0,
                y: data.platformTrends[platform]?.growthRate || 0,
                platform: platform
              })),
              backgroundColor: validPlatforms.map(p => {
                const trend = data.platformTrends[p];
                if (trend?.category === 'ARM') return '#10b981';
                if (trend?.category === 'Linux') return '#3b82f6';
                return '#6b7280';
              }),
              borderColor: '#fff',
              borderWidth: 2,
              pointRadius: 6
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: {
                title: {
                  display: true,
                  text: 'Market Share (%)',
                  color: '#9ca3af'
                },
                ticks: { color: '#9ca3af' },
                grid: { color: '#374151' }
              },
              y: {
                title: {
                  display: true,
                  text: 'Growth Rate (%)',
                  color: '#9ca3af'
                },
                ticks: { color: '#9ca3af' },
                grid: { color: '#374151' }
              }
            },
            plugins: {
              tooltip: {
                callbacks: {
                  label: function(context) {
                    const point = context.parsed;
                    const platform = context.raw.platform;
                    return `${platform}: ${point.x}% share, ${point.y}% growth`;
                  }
                }
              }
            }
          }
        });
      }
    } catch (error) {
      console.error('Error rendering platform trends chart:', error);
    }
  }

  // Growth Trajectory Analysis - Fixed
  if (data.growthTrajectory) {
    console.log('Growth trajectory data:', data.growthTrajectory);
    try {
      const trajectory = data.growthTrajectory;
      const infoElement = document.getElementById('growthTrajectoryInfo');
      if (infoElement) {
        infoElement.innerHTML = `
          <h4 style="margin: 0 0 1rem 0; color: #f1f5f9;">Growth Analysis</h4>
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
            <div>
              <strong>Linear Trend:</strong><br>
              Slope: ${trajectory.linear?.slope || 'N/A'} installs/month<br>
              R²: ${trajectory.linear?.rSquared || 'N/A'} (${trajectory.linear?.trend || 'Unknown'})<br>
              <strong>Prediction:</strong><br>
              Next month: ${trajectory.prediction?.nextMonth || 'N/A'} installs<br>
              Next 3 months: ${trajectory.prediction?.next3Months || 'N/A'} installs
            </div>
            <div>
              <strong>Growth Pattern:</strong><br>
              ${trajectory.exponential?.trend || 'Unknown'}<br>
              Fit Quality: ${trajectory.exponential?.fitQuality || 'N/A'}<br>
              <strong>Recommendation:</strong><br>
              ${(trajectory.linear?.rSquared || 0) > 0.8 ? 'Predictable growth' : 'High volatility'}<br>
              ${(trajectory.linear?.slope || 0) > 0 ? 'Positive trajectory' : 'Need intervention'}
            </div>
          </div>
        `;
      }
    } catch (error) {
      console.error('Error rendering growth trajectory:', error);
    }
  }

  // Version Lifecycle Chart - Fixed
  if (data.versionMigration && typeof data.versionMigration === 'object') {
    try {
      const activeVersions = Object.entries(data.versionMigration)
        .filter(([v, vData]) => vData && vData.currentInstalls > 0)
        .sort(([,a], [,b]) => b.currentInstalls - a.currentInstalls)
        .slice(0, 8);
      
      if (activeVersions.length > 0) {
        createOrUpdateChart('#lifecycleChart', {
          type: 'bubble',
          data: {
            datasets: [{
              label: 'Version Lifecycle',
              data: activeVersions.map(([version, vData]) => ({
                x: vData.adoptionMonths || 0,
                y: vData.currentInstalls || 0,
                r: Math.sqrt(vData.peakValue || 0) / 3 || 5,
                version: version
              })),
              backgroundColor: 'rgba(59, 130, 246, 0.6)',
              borderColor: '#3b82f6',
              borderWidth: 1
            }]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: {
                title: {
                  display: true,
                  text: 'Adoption Speed (months)',
                  color: '#9ca3af'
                },
                ticks: { color: '#9ca3af' },
                grid: { color: '#374151' }
              },
              y: {
                title: {
                  display: true,
                  text: 'Current Installs',
                  color: '#9ca3af'
                },
                ticks: { color: '#9ca3af' },
                grid: { color: '#374151' }
              }
            },
            plugins: {
              tooltip: {
                callbacks: {
                  label: function(context) {
                    const point = context.parsed;
                    const version = context.raw.version;
                    return `${version}: ${point.y} installs, ${point.x} months to adopt`;
                  }
                }
              }
            }
          }
        });
      }
    } catch (error) {
      console.error('Error rendering lifecycle chart:', error);
    }
  }
}
