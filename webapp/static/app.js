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
            github_url: '',
            hevo_github_url: '',
            fivetran_urls: {
                setup_guide_url: '',
                connector_overview_url: '',
                schema_info_url: ''
            },
            description: '',
            manual_file: null,
            manual_text: ''
        },
        
        // 📚 Knowledge Vault State
        showVaultUploadModal: false,
        isUploadingVault: false,
        vaultStats: { connectors: [], connector_count: 0, total_chunks: 0 },
        vaultUploadType: 'text',  // 'text', 'url', 'file', or 'bulk'
        vaultUpload: {
            connector_name: '',
            title: '',
            source_type: 'official_docs',
            content: '',
            url: '',
            file: null,
            files: null  // For bulk upload (FileList)
        },
        bulkUploadProgress: null,  // Progress tracking for bulk uploads
        bulkUploadJobId: null,
        vaultUploadStatus: null,  // Status messages for single uploads
        
        // Confirmation Modal State
        showConfirmModal: false,
        confirmModal: {
            title: '',
            message: '',
            confirmText: 'Confirm',
            cancelText: 'Cancel',
            onConfirm: null,
            type: 'info'  // 'info', 'warning', 'danger'
        },
        
        // App-wide Status Messages
        appStatus: null,  // { type: 'success'|'error'|'info', title: '', message: '' }
        
        // Stop-the-Line Modal State
        showStopTheLineModal: false,
        stopTheLineConnector: null,
        
        // Citation Intervention Modal State
        showCitationModal: false,
        citationConnector: null,
        citationReport: null,
        citationEvidenceMap: {},

        // Initialize
        async init() {
            await this.loadConnectors();
        },
        
        // =====================
        // Utility Methods
        // =====================
        
        showConfirm(title, message, confirmText = 'Confirm', cancelText = 'Cancel', type = 'info') {
            return new Promise((resolve) => {
                this.confirmModal = {
                    title,
                    message,
                    confirmText,
                    cancelText,
                    type,
                    onConfirm: (confirmed) => {
                        this.showConfirmModal = false;
                        resolve(confirmed);
                    }
                };
                this.showConfirmModal = true;
            });
        },
        
        showStatus(type, title, message, autoClear = false) {
            this.appStatus = { type, title, message };
            if (autoClear) {
                setTimeout(() => {
                    if (this.appStatus?.type === type) {
                        this.appStatus = null;
                    }
                }, 5000);
            }
        },
        
        clearStatus() {
            this.appStatus = null;
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
                    if (this.newConnector.github_url) {
                        formData.append('github_url', this.newConnector.github_url);
                    }
                    if (this.newConnector.hevo_github_url) {
                        formData.append('hevo_github_url', this.newConnector.hevo_github_url);
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
                            github_url: this.newConnector.github_url || null,
                            hevo_github_url: this.newConnector.hevo_github_url || null,
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
                    const startResearch = await this.showConfirm(
                        'Connector Created!',
                        'Connector created successfully. Would you like to start research generation now?',
                        'Start Research',
                        'Later',
                        'info'
                    );
                    
                    if (startResearch) {
                        this.startResearch(connector.id);
                    } else {
                        this.showStatus('success', 'Connector Created', `"${connector.name}" has been created. You can start research anytime.`, true);
                    }
                } else {
                    const error = await response.json();
                    this.showStatus('error', 'Creation Failed', error.detail || 'Failed to create connector');
                }
            } catch (error) {
                console.error('Create connector error:', error);
                this.showStatus('error', 'Creation Error', 'Failed to create connector: ' + error.message);
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
                    this.showStatus('error', 'Invalid File Type', 'Please upload a CSV or PDF file.');
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
                        this.showStatus('success', 'Research Started', `Research generation has started for "${connector.name}". Progress will be shown below.`, true);
                    }
                    
                    // Start polling for progress
                    this.pollResearchProgress(connectorId);
                } else {
                    const error = await response.json();
                    this.showStatus('error', 'Research Failed', error.detail || 'Failed to start research');
                }
            } catch (error) {
                console.error('Start research error:', error);
                this.showStatus('error', 'Research Error', 'Failed to start research: ' + error.message);
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
                        } else if (status.status === 'stopped') {
                            // Show stop-the-line notification
                            if (status.progress?.stop_the_line_events?.length > 0) {
                                this.showStatus('error', 'Research Stopped', 
                                    `Research stopped due to critical issues. Section ${status.progress.stop_the_line_events[0].section_number}: ${status.progress.stop_the_line_events[0].reason}`, 
                                    false);
                            }
                            // Stop polling
                        }
                    }
                } catch (error) {
                    console.error('Poll progress error:', error);
                }
            };
            
            poll();
        },
        
        async cancelResearch(connectorId) {
            const confirmed = await this.showConfirm(
                'Cancel Research',
                'Are you sure you want to cancel this research? This action cannot be undone.',
                'Cancel Research',
                'Keep Running',
                'warning'
            );
            
            if (!confirmed) return;
            
            try {
                const response = await fetch(`/api/connectors/${connectorId}/cancel`, {
                    method: 'POST'
                });
                
                if (response.ok) {
                    const connector = this.connectors.find(c => c.id === connectorId);
                    if (connector) {
                        connector.status = 'cancelled';
                        this.showStatus('info', 'Research Cancelled', `Research for "${connector.name}" has been cancelled.`, true);
                    }
                }
            } catch (error) {
                console.error('Cancel research error:', error);
            }
        },
        
        async deleteConnector(connectorId) {
            const connector = this.connectors.find(c => c.id === connectorId);
            const connectorName = connector?.name || 'this connector';
            
            const confirmed = await this.showConfirm(
                'Delete Connector',
                `Are you sure you want to delete "${connectorName}"? This action cannot be undone.`,
                'Delete',
                'Cancel',
                'danger'
            );
            
            if (!confirmed) return;
            
            try {
                const response = await fetch(`/api/connectors/${connectorId}`, {
                    method: 'DELETE'
                });
                
                if (response.ok) {
                    this.connectors = this.connectors.filter(c => c.id !== connectorId);
                    this.showStatus('success', 'Connector Deleted', 'The connector has been successfully deleted.', true);
                } else {
                    const error = await response.json();
                    this.showStatus('error', 'Delete Failed', error.detail || 'Failed to delete connector');
                }
            } catch (error) {
                console.error('Delete connector error:', error);
                this.showStatus('error', 'Delete Error', 'Failed to delete connector: ' + error.message);
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
                    if (data.results.length === 0) {
                        this.showStatus('info', 'No Results', 'No results found for your search query.', true);
                    }
                } else {
                    const error = await response.json();
                    console.error('Search failed:', error);
                    this.showStatus('error', 'Search Failed', error.detail || 'Search request failed');
                }
            } catch (error) {
                console.error('Search error:', error);
                this.showStatus('error', 'Search Error', 'Search failed: ' + error.message);
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
        },

        // =====================
        // 📚 Knowledge Vault Methods
        // =====================
        
        async loadVaultStats() {
            try {
                const response = await fetch('/api/vault/stats');
                if (response.ok) {
                    this.vaultStats = await response.json();
                }
            } catch (error) {
                console.error('Error loading vault stats:', error);
            }
        },
        
        async submitVaultUpload() {
            if (!this.vaultUpload.connector_name) return;
            
            this.isUploadingVault = true;
            
            try {
                let response;
                
                if (this.vaultUploadType === 'text') {
                    // Text upload
                    if (!this.vaultUpload.content) {
                        this.vaultUploadStatus = {
                            type: 'error',
                            title: 'Validation Error',
                            message: 'Please enter documentation content'
                        };
                        this.isUploadingVault = false;
                        return;
                    }
                    
                    response = await fetch('/api/vault/index', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            connector_name: this.vaultUpload.connector_name,
                            title: this.vaultUpload.title || 'Documentation',
                            content: this.vaultUpload.content,
                            source_type: this.vaultUpload.source_type
                        })
                    });
                    
                } else if (this.vaultUploadType === 'url') {
                    // URL fetch
                    if (!this.vaultUpload.url) {
                        this.vaultUploadStatus = {
                            type: 'error',
                            title: 'Validation Error',
                            message: 'Please enter a documentation URL'
                        };
                        this.isUploadingVault = false;
                        return;
                    }
                    
                    response = await fetch('/api/vault/index-url', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            connector_name: this.vaultUpload.connector_name,
                            url: this.vaultUpload.url,
                            source_type: this.vaultUpload.source_type
                        })
                    });
                    
                } else if (this.vaultUploadType === 'file') {
                    // Single file upload
                    if (!this.vaultUpload.file) {
                        this.vaultUploadStatus = {
                            type: 'error',
                            title: 'Validation Error',
                            message: 'Please select a file'
                        };
                        this.isUploadingVault = false;
                        return;
                    }
                    
                    const formData = new FormData();
                    formData.append('connector_name', this.vaultUpload.connector_name);
                    formData.append('title', this.vaultUpload.title || this.vaultUpload.file.name);
                    formData.append('source_type', this.vaultUpload.source_type);
                    formData.append('file', this.vaultUpload.file);
                    
                    response = await fetch('/api/vault/index-file', {
                        method: 'POST',
                        body: formData
                    });
                    
                } else if (this.vaultUploadType === 'bulk') {
                    // Bulk upload (500+ files)
                    if (!this.vaultUpload.files || this.vaultUpload.files.length === 0) {
                        this.vaultUploadStatus = {
                            type: 'error',
                            title: 'Validation Error',
                            message: 'Please select files to upload'
                        };
                        this.isUploadingVault = false;
                        return;
                    }
                    
                    const formData = new FormData();
                    formData.append('connector_name', this.vaultUpload.connector_name);
                    formData.append('source_type', this.vaultUpload.source_type);
                    
                    // Add all files
                    for (const file of this.vaultUpload.files) {
                        formData.append('files', file);
                    }
                    
                    // Start bulk upload (background processing)
                    response = await fetch('/api/vault/bulk-upload', {
                        method: 'POST',
                        body: formData
                    });
                    
                    if (response.ok) {
                        const data = await response.json();
                        this.bulkUploadJobId = data.job_id;
                        
                        // Initialize progress object
                        this.bulkUploadProgress = {
                            job_id: data.job_id,
                            connector_name: data.connector_name,
                            total_files: data.total_files,
                            processed_files: 0,
                            successful_files: 0,
                            failed_files: 0,
                            total_chunks: 0,
                            status: 'processing',
                            current_file: '',
                            errors: [],
                            errorsExpanded: false
                        };
                        
                        // Start polling for progress
                        this.pollBulkUploadProgress();
                        
                        // Close modal but keep progress visible
                        this.showVaultUploadModal = false;
                        this.vaultUploadStatus = {
                            type: 'info',
                            title: 'Bulk Upload Started',
                            message: `Processing ${data.total_files} files in background. Track progress below.`
                        };
                        return;
                    }
                }
                
                if (response && response.ok) {
                    const data = await response.json();
                    this.vaultUploadStatus = {
                        type: 'success',
                        title: 'Upload Successful',
                        message: `Successfully indexed ${data.chunk_count || data.total_chunks || 0} chunks for ${data.connector_name}!`,
                        details: `Connector: ${data.connector_name} | Chunks: ${data.chunk_count || data.total_chunks || 0}`
                    };
                    
                    // Reset form
                    this.resetVaultUploadForm();
                    this.showVaultUploadModal = false;
                    
                    // Reload stats
                    await this.loadVaultStats();
                    
                    // Auto-clear success message after 5 seconds
                    setTimeout(() => {
                        if (this.vaultUploadStatus?.type === 'success') {
                            this.vaultUploadStatus = null;
                        }
                    }, 5000);
                } else if (response) {
                    const error = await response.json();
                    this.vaultUploadStatus = {
                        type: 'error',
                        title: 'Upload Failed',
                        message: error.detail || 'Failed to index documentation',
                        details: error.detail || 'Unknown error occurred'
                    };
                }
            } catch (error) {
                console.error('Vault upload error:', error);
                this.vaultUploadStatus = {
                    type: 'error',
                    title: 'Upload Error',
                    message: error.message || 'An unexpected error occurred',
                    details: error.stack || 'Please check the console for more details'
                };
            } finally {
                this.isUploadingVault = false;
            }
        },
        
        resetVaultUploadForm() {
            this.vaultUpload = {
                connector_name: '',
                title: '',
                source_type: 'official_docs',
                content: '',
                url: '',
                file: null,
                files: null
            };
            // Don't clear bulkUploadProgress - let user see final status
            // Only clear if explicitly requested
            this.bulkUploadJobId = null;
        },
        
        clearVaultStatus() {
            this.vaultUploadStatus = null;
            this.bulkUploadProgress = null;
            this.bulkUploadJobId = null;
        },
        
        async pollBulkUploadProgress() {
            if (!this.bulkUploadJobId) return;
            
            try {
                const response = await fetch(`/api/vault/bulk-upload/${this.bulkUploadJobId}`);
                if (response.ok) {
                    const progressData = await response.json();
                    
                    // Merge with existing progress to preserve errorsExpanded
                    // Auto-expand errors if there are failures
                    const hasErrors = progressData.errors && progressData.errors.length > 0;
                    this.bulkUploadProgress = {
                        ...progressData,
                        errorsExpanded: this.bulkUploadProgress?.errorsExpanded || hasErrors
                    };
                    
                    // Scroll to progress section if it's visible
                    this.$nextTick(() => {
                        const progressSection = document.querySelector('[x-show*="bulkUploadProgress"]');
                        if (progressSection && this.bulkUploadProgress.status === 'processing') {
                            progressSection.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                        }
                    });
                    
                    // Continue polling if not completed
                    if (this.bulkUploadProgress.status === 'processing') {
                        setTimeout(() => this.pollBulkUploadProgress(), 1000);
                    } else if (this.bulkUploadProgress.status === 'completed') {
                        // Update status message
                        this.vaultUploadStatus = {
                            type: this.bulkUploadProgress.failed_files > 0 ? 'error' : 'success',
                            title: this.bulkUploadProgress.failed_files > 0 
                                ? 'Bulk Upload Completed with Errors' 
                                : 'Bulk Upload Completed',
                            message: `Processed ${this.bulkUploadProgress.processed_files}/${this.bulkUploadProgress.total_files} files. ${this.bulkUploadProgress.successful_files} successful, ${this.bulkUploadProgress.failed_files} failed.`,
                            details: `Total chunks created: ${this.bulkUploadProgress.total_chunks}`
                        };
                        
                        // Reload stats
                        await this.loadVaultStats();
                    } else if (this.bulkUploadProgress.status === 'failed') {
                        this.vaultUploadStatus = {
                            type: 'error',
                            title: 'Bulk Upload Failed',
                            message: 'The bulk upload job failed. Check errors below for details.',
                            details: this.bulkUploadProgress.errors?.join('; ') || 'Unknown error'
                        };
                    }
                } else {
                    // Stop polling on error
                    this.vaultUploadStatus = {
                        type: 'error',
                        title: 'Progress Check Failed',
                        message: 'Unable to fetch upload progress. The upload may still be processing.',
                        details: `Job ID: ${this.bulkUploadJobId}`
                    };
                }
            } catch (error) {
                console.error('Error polling progress:', error);
                this.vaultUploadStatus = {
                    type: 'error',
                    title: 'Progress Check Error',
                    message: 'Error checking upload progress: ' + error.message
                };
            }
        },
        
        handleBulkFileUpload(event) {
            this.vaultUpload.files = event.target.files;
        },
        
        handleVaultFileUpload(event) {
            const file = event.target.files[0];
            if (file) {
                this.vaultUpload.file = file;
                if (!this.vaultUpload.title) {
                    this.vaultUpload.title = file.name;
                }
            }
        },
        
        async searchVault(connectorName) {
            const query = prompt(`Search ${connectorName} knowledge vault:`, '');
            if (!query) return;
            
            try {
                const response = await fetch('/api/vault/search', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        connector_name: connectorName,
                        query: query,
                        top_k: 5
                    })
                });
                
                if (response.ok) {
                    const results = await response.json();
                    if (results.length > 0) {
                        this.showStatus('success', 'Search Results', `Found ${results.length} results in the Knowledge Vault.`, true);
                        // Results are displayed in the UI, no need for alert
                    } else {
                        this.showStatus('info', 'No Results', 'No results found in the Knowledge Vault for your query.', true);
                    }
                }
            } catch (error) {
                console.error('Search error:', error);
                this.showStatus('error', 'Search Failed', 'Search failed: ' + error.message);
            }
        },
        
        async deleteVaultKnowledge(connectorName) {
            const confirmed = await this.showConfirm(
                'Delete Knowledge',
                `Are you sure you want to delete all knowledge for "${connectorName}"? This action cannot be undone.`,
                'Delete',
                'Cancel',
                'danger'
            );
            
            if (!confirmed) return;
            
            try {
                const response = await fetch(`/api/vault/${encodeURIComponent(connectorName)}`, {
                    method: 'DELETE'
                });
                
                if (response.ok) {
                    this.showStatus('success', 'Knowledge Deleted', `All knowledge for "${connectorName}" has been deleted.`, true);
                    await this.loadVaultStats();
                } else {
                    this.showStatus('error', 'Delete Failed', 'Failed to delete knowledge from the vault.');
                }
            } catch (error) {
                console.error('Delete error:', error);
                this.showStatus('error', 'Delete Error', 'Error: ' + error.message);
            }
        },
        
        // Stop-the-Line Modal Functions
        showStopTheLineModal(connector) {
            this.stopTheLineConnector = connector;
            this.showStopTheLineModal = true;
        },
        
        async resumeResearch(connectorId) {
            if (!connectorId) return;
            
            try {
                const response = await fetch(`/api/connectors/${connectorId}/research`, {
                    method: 'POST'
                });
                
                if (response.ok) {
                    this.showStopTheLineModal = false;
                    this.showStatus('info', 'Research Resumed', 'Research generation has been resumed. Review the issues and continue.', true);
                    await this.loadConnectors();
                    this.pollResearchProgress(connectorId);
                } else {
                    const error = await response.json();
                    this.showStatus('error', 'Resume Failed', error.detail || 'Failed to resume research');
                }
            } catch (error) {
                console.error('Resume research error:', error);
                this.showStatus('error', 'Resume Error', 'Failed to resume research: ' + error.message);
            }
        },
        
        // Citation Intervention Functions
        async showCitationIntervention(connector) {
            this.citationConnector = connector;
            
            try {
                const response = await fetch(`/api/connectors/${connector.id}/citation-report`, {
                    method: 'POST'
                });
                
                if (response.ok) {
                    const data = await response.json();
                    this.citationReport = data.report;
                    
                    if (connector.progress?.evidence_map_json) {
                        this.citationEvidenceMap = connector.progress.evidence_map_json;
                    } else {
                        this.citationEvidenceMap = {};
                    }
                    
                    this.showCitationModal = true;
                } else {
                    const error = await response.json();
                    this.showStatus('error', 'Report Failed', error.detail || 'Failed to load citation report');
                }
            } catch (error) {
                console.error('Citation report error:', error);
                this.showStatus('error', 'Report Error', 'Failed to load citation report: ' + error.message);
            }
        },
        
        async applyCitationOverrides(connectorId) {
            if (!connectorId || !this.citationReport) return;
            
            const overrides = [];
            
            if (this.citationReport.uncited_claims) {
                for (const claim of this.citationReport.uncited_claims) {
                    if (claim.action && claim.action !== '') {
                        const override = {
                            claim_id: claim.sentence || `claim_${overrides.length}`,
                            action: claim.action
                        };
                        
                        if (claim.action === 'attach_citation' && claim.citation) {
                            override.citation = claim.citation;
                            const citationTag = claim.citation.replace(/[\[\]]/g, '');
                            if (this.citationEvidenceMap[citationTag]) {
                                override.evidence_id = this.citationEvidenceMap[citationTag].evidence_id;
                            }
                        }
                        
                        overrides.push(override);
                    }
                }
            }
            
            try {
                const response = await fetch(`/api/connectors/${connectorId}/citation-override`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ overrides })
                });
                
                if (response.ok) {
                    this.showCitationModal = false;
                    this.showStatus('success', 'Overrides Applied', 'Citation overrides have been applied. Research will resume.', true);
                    await this.loadConnectors();
                    await this.resumeResearch(connectorId);
                } else {
                    const error = await response.json();
                    this.showStatus('error', 'Override Failed', error.detail || 'Failed to apply citation overrides');
                }
            } catch (error) {
                console.error('Citation override error:', error);
                this.showStatus('error', 'Override Error', 'Failed to apply citation overrides: ' + error.message);
            }
        }
    };
}
