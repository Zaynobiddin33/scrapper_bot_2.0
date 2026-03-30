"""
Robust Stealth Module - CDP-based anti-detection
Injects JavaScript before any page scripts run.
"""

STEALTH_SCRIPT = """
(() => {
    'use strict';
    
    // ===== 1. navigator.webdriver =====
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true,
        enumerable: true
    });
    
    // Remove from prototype chain
    if (navigator.__proto__) {
        delete navigator.__proto__.webdriver;
    }
    
    // ===== 2. Chrome Runtime =====
    if (!window.chrome) {
        window.chrome = {};
    }
    
    // Chrome runtime with all expected methods
    window.chrome.runtime = {
        OnInstalledReason: {CHROME_UPDATE: "chrome_update", INSTALL: "install", SHARED_MODULE_UPDATE: "shared_module_update", UPDATE: "update"},
        OnRestartRequiredReason: {APP_UPDATE: "app_update", OS_UPDATE: "os_update", PERIODIC: "periodic"},
        PlatformArch: {ARM: "arm", ARM64: "arm64", MIPS: "mips", MIPS64: "mips64", X86_32: "x86-32", X86_64: "x86-64"},
        PlatformNaclArch: {ARM: "arm", MIPS: "mips", MIPS64: "mips64", MIPS32: "mips32", X86_32: "x86-32", X86_64: "x86-64"},
        PlatformOs: {ANDROID: "android", CROS: "cros", LINUX: "linux", MAC: "mac", OPENBSD: "openbsd", WIN: "win"},
        RequestUpdateCheckStatus: {NO_UPDATE: "no_update", THROTTLED: "throttled", UPDATE_AVAILABLE: "update_available"},
        connect: function() { return {postMessage: function() {}, disconnect: function() {}, onMessage: {addListener: function() {}}, onDisconnect: {addListener: function() {}}}; },
        sendMessage: function() {},
        onMessage: {addListener: function() {}, removeListener: function() {}},
        onConnect: {addListener: function() {}, removeListener: function() {}},
        onInstalled: {addListener: function() {}},
        onStartup: {addListener: function() {}}
    };
    
    // Chrome app
    window.chrome.app = {
        isInstalled: false,
        InstallState: {DISABLED: "disabled", INSTALLED: "installed", NOT_INSTALLED: "not_installed"},
        RunningState: {CANNOT_RUN: "cannot_run", READY_TO_RUN: "ready_to_run", RUNNING: "running"},
        getIsInstalled: function() { return false; },
        getDetails: function() { return null; },
        getInstallState: function(cb) { cb(window.chrome.app.InstallState.NOT_INSTALLED); },
        getRunningState: function() { return window.chrome.app.RunningState.CANNOT_RUN; }
    };
    
    // Chrome loadTimes
    window.chrome.loadTimes = function() {
        return {
            commitLoadTime: performance.timing.responseStart / 1000,
            connectionInfo: 'h2',
            finishDocumentLoadTime: performance.timing.domContentLoadedEventEnd / 1000,
            finishLoadTime: performance.timing.loadEventEnd / 1000,
            firstPaintAfterLoadTime: 0,
            firstPaintTime: performance.timing.responseStart / 1000 + 0.1,
            navigationType: 'Other',
            npnNegotiatedProtocol: 'h2',
            requestTime: performance.timing.requestStart / 1000,
            startLoadTime: performance.timing.requestStart / 1000,
            wasAlternateProtocolAvailable: false,
            wasFetchedViaSpdy: false,
            wasNpnNegotiated: true
        };
    };
    
    // Chrome csi
    window.chrome.csi = function() {
        return {
            onloadT: Date.now(),
            pageT: Date.now() - performance.timing.navigationStart,
            startE: performance.timing.navigationStart,
            tran: 15
        };
    };
    
    // ===== 3. Permissions API =====
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = function(parameters) {
        if (parameters.name === 'notifications') {
            return Promise.resolve({
                state: Notification.permission,
                addEventListener: function() {},
                removeEventListener: function() {},
                dispatchEvent: function() { return true; }
            });
        }
        return originalQuery.call(this, parameters);
    };
    
    // ===== 4. Plugins =====
    const fakePlugins = [
        {name: "Chrome PDF Plugin", filename: "internal-pdf-viewer", description: "Portable Document Format", version: "undefined", length: 1, item: function(i) { return this; }, namedItem: function(n) { return null; }},
        {name: "Chrome PDF Viewer", filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai", description: "Portable Document Format", version: "undefined", length: 1, item: function(i) { return this; }, namedItem: function(n) { return null; }},
        {name: "Native Client", filename: "internal-nacl-plugin", description: "", version: "undefined", length: 1, item: function(i) { return this; }, namedItem: function(n) { return null; }}
    ];
    
    fakePlugins.length = fakePlugins.length;
    fakePlugins.item = function(index) { return fakePlugins[index] || null; };
    fakePlugins.namedItem = function(name) {
        for (let i = 0; i < fakePlugins.length; i++) {
            if (fakePlugins[i].name === name) return fakePlugins[i];
        }
        return null;
    };
    fakePlugins.refresh = function() {};
    
    Object.defineProperty(navigator, 'plugins', {
        get: function() { return fakePlugins; },
        configurable: true,
        enumerable: true
    });
    
    // MimeTypes
    const fakeMimeTypes = [
        {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: fakePlugins[0]},
        {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: fakePlugins[1]}
    ];
    fakeMimeTypes.length = fakeMimeTypes.length;
    fakeMimeTypes.item = function(index) { return fakeMimeTypes[index] || null; };
    fakeMimeTypes.namedItem = function(name) {
        for (let i = 0; i < fakeMimeTypes.length; i++) {
            if (fakeMimeTypes[i].type === name) return fakeMimeTypes[i];
        }
        return null;
    };
    
    Object.defineProperty(navigator, 'mimeTypes', {
        get: function() { return fakeMimeTypes; },
        configurable: true,
        enumerable: true
    });
    
    // ===== 5. Notification API =====
    if (typeof Notification === 'undefined') {
        window.Notification = function() {};
        Notification.permission = 'default';
        Notification.requestPermission = function() { return Promise.resolve('default'); };
    }
    
    // ===== 6. Battery API =====
    if (!navigator.getBattery) {
        navigator.getBattery = function() {
            return Promise.resolve({
                charging: true,
                chargingTime: 0,
                dischargingTime: Infinity,
                level: 1.0,
                addEventListener: function() {},
                removeEventListener: function() {}
            });
        };
    }
    
    // ===== 7. Connection API =====
    Object.defineProperty(navigator, 'connection', {
        get: function() {
            return {
                effectiveType: '4g',
                rtt: 50,
                downlink: 10,
                saveData: false,
                type: 'wifi',
                onchange: null,
                addEventListener: function() {},
                removeEventListener: function() {}
            };
        },
        configurable: true,
        enumerable: true
    });
    
    // ===== 8. Memory API =====
    Object.defineProperty(navigator, 'deviceMemory', {
        get: function() { return 8; },
        configurable: true,
        enumerable: true
    });
    
    // ===== 9. Keyboard API =====
    Object.defineProperty(navigator, 'keyboard', {
        get: function() { return null; },
        configurable: true,
        enumerable: true
    });
    
    // ===== 10. MediaCapabilities =====
    if (!navigator.mediaCapabilities) {
        navigator.mediaCapabilities = {
            decodingInfo: function() {
                return Promise.resolve({
                    supported: true,
                    smooth: true,
                    powerEfficient: true
                });
            },
            encodingInfo: function() {
                return Promise.resolve({
                    supported: true,
                    smooth: true,
                    powerEfficient: true
                });
            }
        };
    }
    
    // ===== 11. Prevent iframe detection =====
    const originalContentWindow = Object.getOwnPropertyDescriptor(HTMLIFrameElement.prototype, 'contentWindow');
    if (originalContentWindow) {
        Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
            get: function() {
                const win = originalContentWindow.get.call(this);
                if (win) {
                    try {
                        win.chrome = window.chrome;
                    } catch(e) {}
                }
                return win;
            }
        });
    }
    
    // ===== 12. Canvas fingerprint protection =====
    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
    const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
    
    // Add subtle noise to canvas fingerprint (consistent per session)
    CanvasRenderingContext2D.prototype.getImageData = function(x, y, w, h) {
        const imageData = originalGetImageData.call(this, x, y, w, h);
        // Add imperceptible noise
        for (let i = 0; i < imageData.data.length; i += 4) {
            const noise = Math.random() < 0.05 ? 1 : 0;
            imageData.data[i] = Math.min(255, imageData.data[i] + noise);
            imageData.data[i+1] = Math.min(255, imageData.data[i+1] + noise);
            imageData.data[i+2] = Math.min(255, imageData.data[i+2] + noise);
        }
        return imageData;
    };
    
    // ===== 13. WebDriver property cleanup =====
    try {
        const descriptor = Object.getOwnPropertyDescriptor(navigator, 'webdriver');
        if (descriptor && descriptor.get) {
            Object.defineProperty(navigator, 'webdriver', {
                get: function() { return undefined; },
                configurable: true,
                enumerable: true
            });
        }
    } catch(e) {}
    
    // ===== 14. Error stack trace cleanup =====
    const originalToString = Error.prototype.toString;
    Error.prototype.toString = function() {
        let str = originalToString.call(this);
        str = str.replace(/playwright|puppeteer|selenium|webdriver/gi, 'chrome');
        return str;
    };
    
    // ===== 15. Prevent automation detection via console =====
    const originalConsoleLog = console.log;
    console.log = function(...args) {
        // Filter out Playwright-specific messages
        const filtered = args.filter(arg => 
            typeof arg !== 'string' || 
            !arg.match(/playwright|CDP|DevTools Protocol/i)
        );
        if (filtered.length > 0) {
            originalConsoleLog.apply(console, filtered);
        }
    };
    
    // ===== 16. Plugins consistency =====
    Object.defineProperty(navigator, 'plugins', {
        get: function() {
            // Ensure plugins.length > 0 (browsers have plugins)
            return fakePlugins;
        },
        configurable: true,
        enumerable: true
    });
    
    // ===== 17. PDF Viewer check =====
    if (!window.pdfViewerEnabled) {
        Object.defineProperty(window, 'pdfViewerEnabled', {
            get: function() { return true; },
            configurable: true,
            enumerable: true
        });
    }
    
})();
"""


def get_stealth_script():
    """Get the complete stealth script"""
    return STEALTH_SCRIPT
