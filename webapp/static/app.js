/**
 * Connector Research Platform - Alpine.js Application
 */

function dashboard() {
    return {
        // State
        activeTab: 'connectors',
        searchQuery: '',
        searchResults: [],
        isSearching: false,
        chatInput: '',
        chatMessages: [],
        isChatLoading: false,
        selectedConnector: '',
        
        // Connector State
        connectors: [],
        connectorsLoading: false,
        connectorsLoaded: false,
        showNewConnectorModal: false,
        showFivetranInputs: false,
        isCreatingConnector: false,
        newConnector: {
            name: '',
            type: 'rest_api',
            github_url: '',
            fivetran_urls: {
                setup_guide_url: '',
                connector_overview_url: '',
                schema_info_url: ''
            },
            description: '',
            manual_file: null,
            manual_text: ''
        },
        connectorTypes: [
            { id: 'rest_api', label: 'REST' },
            { id: 'graphql', label: 'GraphQL' },
            { id: 'soap', label: 'SOAP' },
            { id: 'jdbc', label: 'JDBC' },
            { id: 'sdk', label: 'SDK' },
            { id: 'webhook', label: 'Webhook' },
            { id: 'advertising', label: 'Ads' },
            { id: 'warehouse', label: 'Warehouse' }
        ],

        // Initialize
        async init() {
            await this.loadConnectors();
        },

        // =====================
        // Connector Methods
        // =====================
        
        async loadConnectors() {
            if (this.connectorsLoaded && this.connectors.length > 0) return;
            
            this.connectorsLoading = true;
            
            try {
                const response = await fetch('/api/connectors');
                if (response.ok) {
                    const data = await response.json();
                    this.connectors = data.connectors;
                    this.connectorsLoaded = true;
                } else {
                    console.error('Failed to load connectors');
                    this.connectors = [];
                }
            } catch (error) {
                console.error('Connectors loading error:', error);
                this.connectors = [];
            } finally {
                this.connectorsLoading = false;
            }
        },
        
        async createConnector() {
            if (!this.newConnector.name) return;
            
            this.isCreatingConnector = true;
            
            try {
                // Build fivetran_urls only if at least one URL is provided
                let fivetranUrls = null;
                const fUrls = this.newConnector.fivetran_urls;
                if (fUrls.setup_guide_url || fUrls.connector_overview_url || fUrls.schema_info_url) {
                    fivetranUrls = {
                        setup_guide_url: fUrls.setup_guide_url || null,
                        connector_overview_url: fUrls.connector_overview_url || null,
                        schema_info_url: fUrls.schema_info_url || null
                    };
                }
                
                // Use FormData if we have a file to upload
                let response;
                if (this.newConnector.manual_file) {
                    const formData = new FormData();
                    formData.append('name', this.newConnector.name);
                    formData.append('connector_type', this.newConnector.type);
                    if (this.newConnector.github_url) {
                        formData.append('github_url', this.newConnector.github_url);
                    }
                    if (fivetranUrls) {
                        formData.append('fivetran_urls', JSON.stringify(fivetranUrls));
                    }
                    if (this.newConnector.manual_text) {
                        formData.append('manual_text', this.newConnector.manual_text);
                    }
                    formData.append('manual_file', this.newConnector.manual_file);
                    
                    response = await fetch('/api/connectors/upload', {
                        method: 'POST',
                        body: formData
                    });
                } else {
                    // Regular JSON request
                    response = await fetch('/api/connectors', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            name: this.newConnector.name,
                            connector_type: this.newConnector.type,
                            github_url: this.newConnector.github_url || null,
                            fivetran_urls: fivetranUrls,
                            description: this.newConnector.description || '',
                            manual_text: this.newConnector.manual_text || null
                        })
                    });
                }
                
                if (response.ok) {
                    const connector = await response.json();
                    this.connectors.push(connector);
                    this.showNewConnectorModal = false;
                    this.showFivetranInputs = false;
                    this.resetNewConnector();
                    
                    // Optionally auto-start research
                    if (confirm('Connector created! Start research generation now?')) {
                        this.startResearch(connector.id);
                    }
                } else {
                    const error = await response.json();
                    alert('Failed to create connector: ' + (error.detail || 'Unknown error'));
                }
            } catch (error) {
                console.error('Create connector error:', error);
                alert('Failed to create connector: ' + error.message);
            } finally {
                this.isCreatingConnector = false;
            }
        },
        
        handleFileUpload(event) {
            const file = event.target.files[0];
            if (file) {
                const allowedTypes = ['text/csv', 'application/pdf', 'application/vnd.ms-excel'];
                const allowedExtensions = ['.csv', '.pdf'];
                
                const extension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
                if (!allowedExtensions.includes(extension)) {
                    alert('Please upload a CSV or PDF file.');
                    event.target.value = '';
                    return;
                }
                
                this.newConnector.manual_file = file;
            }
        },
        
        clearFileUpload() {
            this.newConnector.manual_file = null;
            // Reset any file input elements
            const fileInputs = document.querySelectorAll('input[type="file"]');
            fileInputs.forEach(input => input.value = '');
        },
        
        resetNewConnector() {
            this.newConnector = { 
                name: '', 
                type: 'rest_api', 
                github_url: '', 
                fivetran_urls: {
                    setup_guide_url: '',
                    connector_overview_url: '',
                    schema_info_url: ''
                },
                description: '',
                manual_file: null,
                manual_text: ''
            };
            this.clearFileUpload();
        },
        
        async startResearch(connectorId) {
            try {
                const response = await fetch(`/api/connectors/${connectorId}/generate`, {
                    method: 'POST'
                });
                
                if (response.ok) {
                    // Update local connector status
                    const connector = this.connectors.find(c => c.id === connectorId);
                    if (connector) {
                        connector.status = 'researching';
                    }
                    
                    // Start polling for progress
                    this.pollResearchProgress(connectorId);
                } else {
                    const error = await response.json();
                    alert('Failed to start research: ' + (error.detail || 'Unknown error'));
                }
            } catch (error) {
                console.error('Start research error:', error);
                alert('Failed to start research: ' + error.message);
            }
        },
        
        async pollResearchProgress(connectorId) {
            const poll = async () => {
                try {
                    const response = await fetch(`/api/connectors/${connectorId}/status`);
                    if (response.ok) {
                        const status = await response.json();
                        
                        // Update local connector
                        const connector = this.connectors.find(c => c.id === connectorId);
                        if (connector) {
                            connector.status = status.status;
                            connector.progress = status.progress;
                        }
                        
                        // Continue polling if still running
                        if (status.is_running) {
                            setTimeout(poll, 2000);
                        } else if (status.status === 'complete') {
                            // Refresh connector data
                            this.connectorsLoaded = false;
                            await this.loadConnectors();
                        }
                    }
                } catch (error) {
                    console.error('Poll progress error:', error);
                }
            };
            
            poll();
        },
        
        async cancelResearch(connectorId) {
            if (!confirm('Are you sure you want to cancel this research?')) return;
            
            try {
                const response = await fetch(`/api/connectors/${connectorId}/cancel`, {
                    method: 'POST'
                });
                
                if (response.ok) {
                    const connector = this.connectors.find(c => c.id === connectorId);
                    if (connector) {
                        connector.status = 'cancelled';
                    }
                }
            } catch (error) {
                console.error('Cancel research error:', error);
            }
        },
        
        async deleteConnector(connectorId) {
            if (!confirm('Are you sure you want to delete this connector? This cannot be undone.')) return;
            
            try {
                const response = await fetch(`/api/connectors/${connectorId}`, {
                    method: 'DELETE'
                });
                
                if (response.ok) {
                    this.connectors = this.connectors.filter(c => c.id !== connectorId);
                } else {
                    const error = await response.json();
                    alert('Failed to delete connector: ' + (error.detail || 'Unknown error'));
                }
            } catch (error) {
                console.error('Delete connector error:', error);
                alert('Failed to delete connector: ' + error.message);
            }
        },
        
        viewConnectorResearch(connectorId) {
            // Open the beautiful HTML research viewer in a new tab
            window.open(`/connectors/${connectorId}/view`, '_blank');
        },
        
        searchConnector(connectorId) {
            this.selectedConnector = connectorId;
            this.activeTab = 'search';
            const connector = this.connectors.find(c => c.id === connectorId);
            if (connector) {
                this.searchQuery = '';
                // Focus the search input
                this.$nextTick(() => {
                    const input = document.querySelector('input[placeholder*="Search"]');
                    if (input) input.focus();
                });
            }
        },

        // =====================
        // Search Methods
        // =====================
        
        async performSearch() {
            if (!this.searchQuery.trim()) return;

            this.isSearching = true;
            this.searchResults = [];

            try {
                const payload = {
                    query: this.searchQuery,
                    top_k: 10
                };

                if (this.selectedConnector) {
                    payload.connector_id = this.selectedConnector;
                }

                const response = await fetch('/api/search', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                if (response.ok) {
                    const data = await response.json();
                    this.searchResults = data.results;
                } else {
                    const error = await response.json();
                    console.error('Search failed:', error);
                    alert('Search failed: ' + (error.detail || 'Unknown error'));
                }
            } catch (error) {
                console.error('Search error:', error);
                alert('Search failed: ' + error.message);
            } finally {
                this.isSearching = false;
            }
        },

        // =====================
        // Chat Methods
        // =====================
        
        async sendMessage(predefinedMessage = null) {
            const message = predefinedMessage || this.chatInput.trim();
            if (!message) return;

            // Add user message
            this.chatMessages.push({
                role: 'user',
                content: message
            });

            this.chatInput = '';
            this.isChatLoading = true;

            // Scroll to bottom
            this.$nextTick(() => {
                const container = this.$refs.chatContainer;
                if (container) {
                    container.scrollTop = container.scrollHeight;
                }
            });

            try {
                const payload = {
                    message: message,
                    top_k: 5
                };

                if (this.selectedConnector) {
                    payload.connector_id = this.selectedConnector;
                }

                const response = await fetch('/api/chat', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                if (response.ok) {
                    const data = await response.json();
                    this.chatMessages.push({
                        role: 'assistant',
                        content: data.answer,
                        sources: data.sources || []
                    });
                } else {
                    const error = await response.json();
                    this.chatMessages.push({
                        role: 'assistant',
                        content: 'Sorry, I encountered an error: ' + (error.detail || 'Unknown error'),
                        sources: []
                    });
                }
            } catch (error) {
                console.error('Chat error:', error);
                this.chatMessages.push({
                    role: 'assistant',
                    content: 'Sorry, I encountered an error: ' + error.message,
                    sources: []
                });
            } finally {
                this.isChatLoading = false;

                // Scroll to bottom
                this.$nextTick(() => {
                    const container = this.$refs.chatContainer;
                    if (container) {
                        container.scrollTop = container.scrollHeight;
                    }
                });
            }
        },

        // Render markdown to HTML
        renderMarkdown(text) {
            if (!text) return '';
            try {
                if (typeof marked !== 'undefined') {
                    return marked.parse(text);
                }
                return text.replace(/\n/g, '<br>');
            } catch (e) {
                return text.replace(/\n/g, '<br>');
            }
        }
    };
}
