"""
Simplified Health Check Module - Robust Import Handling
Works around circular import and module structure issues.

ENHANCEMENTS:
- Detailed LLM connectivity diagnostics
- API key validation
- URL connectivity testing via socket connection
- SSL certificate verification
- OpenAI module import checking
- Granular error classification with remediation suggestions
"""

import os
import time
import logging
import socket
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, Any

logger = logging.getLogger(__name__)

class HealthChecker:
    """Tracks health status and caches results."""
    
    def __init__(self, cache_ttl_seconds: int = 30):
        self.cache_ttl = timedelta(seconds=cache_ttl_seconds)
        self._cache = {}
        self._last_check = {}
    
    def is_cached_valid(self, key: str) -> bool:
        if key not in self._last_check:
            return False
        return datetime.now() - self._last_check[key] < self.cache_ttl
    
    def get_cached(self, key: str) -> Any:
        if self.is_cached_valid(key):
            return self._cache.get(key)
        return None
    
    def set_cache(self, key: str, value: Any) -> None:
        self._cache[key] = value
        self._last_check[key] = datetime.now()
    
    def check_environment(self) -> Dict[str, Any]:
        """Check required environment variables are set."""
        checks = {
            "DEEPTHOUGHT_API_KEY": bool(os.environ.get("DEEPTHOUGHT_API_KEY")),
            "GCS_BUCKET_NAME": bool(os.environ.get("GCS_BUCKET_NAME")),
        }
        
        all_present = all(checks.values())
        missing = [k for k, v in checks.items() if not v]
        
        return {
            "status": "ok" if all_present else "missing_vars",
            "checks": checks,
            "missing": missing,
        }
    
    def check_gcs_connectivity(self) -> Dict[str, Any]:
        """Test GCS bucket access."""
        try:
            from google.cloud import storage
            from google.oauth2 import service_account
            import json
            
            bucket_name = os.environ.get("GCS_BUCKET_NAME")
            if not bucket_name:
                return {"status": "error", "error": "GCS_BUCKET_NAME not set"}
            
            # Try to get GCS client
            creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if creds_json:
                creds_info = json.loads(creds_json)
                creds = service_account.Credentials.from_service_account_info(creds_info)
                client = storage.Client(credentials=creds)
            else:
                client = storage.Client()
            
            bucket = client.bucket(bucket_name)
            exists = bucket.exists()
            
            return {
                "status": "ok" if exists else "bucket_not_found",
                "bucket": bucket_name,
                "bucket_exists": exists,
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)[:100],
            }
    
    def _diagnose_api_key(self) -> Dict[str, Any]:
        """Diagnose API key validity."""
        api_key = os.environ.get("DEEPTHOUGHT_API_KEY")
        
        if not api_key:
            return {"valid": False, "reason": "API key not set"}
        
        if len(api_key) < 10:
            return {"valid": False, "reason": "API key too short"}
        
        if not isinstance(api_key, str):
            return {"valid": False, "reason": f"API key is not a string: {type(api_key)}"}
        
        # Check for common key prefixes/formats
        if api_key.startswith("sk-") or api_key.startswith("Bearer "):
            return {"valid": True, "format": "OpenAI-compatible"}
        
        return {"valid": True, "format": "custom"}
    
    def _diagnose_url_connectivity(self, base_url: str) -> Dict[str, Any]:
        """Test if base URL is reachable."""
        try:
            parsed = urllib.parse.urlparse(base_url)
            hostname = parsed.hostname
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            
            if not hostname:
                return {"reachable": False, "error": "Could not parse hostname from URL"}
            
            # Try socket connection
            start_time = time.time()
            try:
                sock = socket.create_connection((hostname, port), timeout=5)
                sock.close()
                elapsed = time.time() - start_time
                return {
                    "reachable": True,
                    "hostname": hostname,
                    "port": port,
                    "connection_time_ms": round(elapsed * 1000, 2),
                }
            except socket.gaierror as e:
                return {
                    "reachable": False,
                    "error": f"DNS resolution failed for {hostname}: {str(e)[:60]}",
                    "error_type": "dns_resolution"
                }
            except socket.timeout:
                return {
                    "reachable": False,
                    "error": f"Connection timeout to {hostname}:{port}",
                    "error_type": "timeout",
                }
            except ConnectionRefusedError:
                return {
                    "reachable": False,
                    "error": f"Connection refused to {hostname}:{port}",
                    "error_type": "connection_refused",
                }
            except OSError as e:
                return {
                    "reachable": False,
                    "error": f"OS error connecting to {hostname}:{port}: {str(e)[:60]}",
                    "error_type": "os_error",
                }
        except Exception as e:
            return {
                "reachable": False,
                "error": f"Unexpected error in URL connectivity check: {str(e)[:60]}",
            }
    
    def _diagnose_openai_import(self) -> Dict[str, Any]:
        """Check if openai module is available and importable."""
        try:
            import openai
            version = getattr(openai, "__version__", "unknown")
            return {
                "importable": True,
                "version": version,
                "module_path": openai.__file__,
            }
        except ImportError as e:
            return {
                "importable": False,
                "error": f"ImportError: {str(e)[:80]}",
                "suggestion": "Install with: pip install openai",
            }
        except Exception as e:
            return {
                "importable": False,
                "error": f"Unexpected error importing openai: {str(e)[:80]}",
            }
    
    def _diagnose_ssl_certificate(self, base_url: str) -> Dict[str, Any]:
        """Test SSL certificate validity for HTTPS URLs."""
        import ssl
        
        parsed = urllib.parse.urlparse(base_url)
        if parsed.scheme != "https":
            return {"applicable": False, "reason": "Not HTTPS URL"}
        
        hostname = parsed.hostname
        port = parsed.port or 443
        
        try:
            context = ssl.create_default_context()
            with socket.create_connection((hostname, port), timeout=5) as sock:
                with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                    cert = ssock.getpeercert()
                    return {
                        "valid": True,
                        "subject": str(cert.get("subject", "N/A"))[:100],
                        "issuer": str(cert.get("issuer", "N/A"))[:100],
                    }
        except ssl.SSLError as e:
            return {
                "valid": False,
                "error": f"SSL certificate error: {str(e)[:80]}",
                "error_type": "ssl_error",
                "remediation": "Check certificate validity or disable certificate verification if in test environment",
            }
        except socket.gaierror:
            return {
                "valid": False,
                "error": "Cannot resolve hostname (DNS issue)",
                "error_type": "dns_error",
            }
        except Exception as e:
            return {
                "valid": False,
                "error": f"Certificate check failed: {str(e)[:80]}",
            }
    
    def _diagnose_timeout_details(self, base_url: str) -> Dict[str, Any]:
        """Detailed timeout diagnosis - test connectivity at different stages."""
        parsed = urllib.parse.urlparse(base_url)
        hostname = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        
        results = {
            "hostname": hostname,
            "port": port,
            "scheme": parsed.scheme,
            "tests": {}
        }
        
        # Test 1: DNS Resolution Speed
        try:
            start = time.time()
            ip = socket.gethostbyname(hostname)
            dns_time = time.time() - start
            results["tests"]["dns_resolution"] = {
                "success": True,
                "ip": ip,
                "time_ms": round(dns_time * 1000, 2)
            }
        except socket.gaierror as e:
            results["tests"]["dns_resolution"] = {
                "success": False,
                "error": str(e)[:60]
            }
            return results  # Can't proceed without DNS
        
        # Test 2: TCP Connect (raw socket, no TLS)
        try:
            start = time.time()
            sock = socket.create_connection((hostname, port), timeout=5)
            connect_time = time.time() - start
            sock.close()
            results["tests"]["tcp_connect"] = {
                "success": True,
                "time_ms": round(connect_time * 1000, 2),
                "note": "TCP connection successful"
            }
        except socket.timeout:
            results["tests"]["tcp_connect"] = {
                "success": False,
                "error": "TCP connection timeout (5s)",
                "likely_cause": "Service not responding or firewall blocking"
            }
            return results  # TCP failed, no point testing TLS
        except ConnectionRefusedError:
            results["tests"]["tcp_connect"] = {
                "success": False,
                "error": "Connection refused",
                "likely_cause": "Service not listening on this port"
            }
            return results
        except Exception as e:
            results["tests"]["tcp_connect"] = {
                "success": False,
                "error": str(e)[:60]
            }
            return results
        
        # Test 3: TLS Handshake (if HTTPS)
        if parsed.scheme == "https":
            try:
                import ssl
                start = time.time()
                context = ssl.create_default_context()
                with socket.create_connection((hostname, port), timeout=5) as sock:
                    with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                        tls_time = time.time() - start
                        results["tests"]["tls_handshake"] = {
                            "success": True,
                            "time_ms": round(tls_time * 1000, 2)
                        }
            except socket.timeout:
                results["tests"]["tls_handshake"] = {
                    "success": False,
                    "error": "TLS handshake timeout",
                    "likely_cause": "Server slow at TLS negotiation"
                }
            except ssl.SSLError as e:
                results["tests"]["tls_handshake"] = {
                    "success": False,
                    "error": f"TLS error: {str(e)[:50]}",
                    "error_type": "ssl_error"
                }
            except Exception as e:
                results["tests"]["tls_handshake"] = {
                    "success": False,
                    "error": str(e)[:60]
                }
        
        return results
    
    def check_lvm_connectivity(self, detailed: bool = True) -> Dict[str, Any]:
        """
        Test LLM connectivity with detailed diagnostics.
        Handles various import scenarios and module structures.
        
        Args:
            detailed: If True, runs comprehensive diagnostic checks
        """
        api_key = os.environ.get("DEEPTHOUGHT_API_KEY")
        base_url = "https://dtcontroller.sr.unh.edu:4242/openai/v1"
        model = "ets:aws:us.anthropic.claude-haiku-4-5-20251001-v1:0"
        
        diagnostics = {}
        
        # Step 1: Check API key
        if detailed:
            api_key_diag = self._diagnose_api_key()
            diagnostics["api_key"] = api_key_diag
            if not api_key_diag.get("valid"):
                return {
                    "status": "error",
                    "error": "API key validation failed",
                    "diagnostics": diagnostics,
                }
        
        if not api_key:
            return {
                "status": "error",
                "error": "DEEPTHOUGHT_API_KEY not set",
            }
        
        # Step 2: Check openai module
        if detailed:
            openai_diag = self._diagnose_openai_import()
            diagnostics["openai_module"] = openai_diag
            if not openai_diag.get("importable"):
                return {
                    "status": "error",
                    "error": "openai module not available",
                    "diagnostics": diagnostics,
                }
        
        # Step 3: Check URL connectivity
        if detailed:
            url_diag = self._diagnose_url_connectivity(base_url)
            diagnostics["url_connectivity"] = url_diag
            if not url_diag.get("reachable"):
                # If timeout, run detailed timeout diagnostics
                if url_diag.get("error_type") == "timeout":
                    timeout_diag = self._diagnose_timeout_details(base_url)
                    diagnostics["timeout_details"] = timeout_diag
                return {
                    "status": "unreachable",
                    "error": f"Cannot reach {base_url}",
                    "diagnostics": diagnostics,
                }
        
        # Step 4: Check SSL certificate (for HTTPS)
        if detailed:
            ssl_diag = self._diagnose_ssl_certificate(base_url)
            diagnostics["ssl_certificate"] = ssl_diag
            if not ssl_diag.get("applicable") and ssl_diag.get("valid") is False:
                return {
                    "status": "error",
                    "error": "SSL certificate validation failed",
                    "diagnostics": diagnostics,
                }
        
        # Step 5: Make actual API call
        try:
            import openai
            
            # Try to get client from app module first (avoids re-initializing)
            get_client = None
            try:
                import sys
                if 'app' in sys.modules:
                    get_client = getattr(sys.modules['app'], 'get_openai_client', None)
            except Exception:
                pass
            
            # If can't get from app module, create client directly
            if not get_client:
                client = openai.OpenAI(
                    api_key=api_key,
                    base_url=base_url,
                    max_retries=0,
                    timeout=5.0
                )
            else:
                try:
                    client = get_client()
                except Exception:
                    # If get_client fails, create directly
                    client = openai.OpenAI(
                        api_key=api_key,
                        base_url=base_url,
                        max_retries=0,
                        timeout=5.0
                    )
            
            # Make minimal API call
            response = client.chat.completions.create(
                model=model,
                max_tokens=10,
                messages=[{"role": "user", "content": "ok"}],
            )
            
            result = {
                "status": "ok",
                "service_reachable": True,
                "model": getattr(response, 'model', 'unknown'),
            }
            if detailed:
                result["diagnostics"] = diagnostics
            return result
            
        except openai.APIStatusError as e:
            error_detail = {
                "status": "api_error",
                "status_code": e.status_code,
                "error_message": str(e.message) if hasattr(e, 'message') else str(e)[:100],
                "error_type": "api_status_error",
            }
            
            if e.status_code == 429:
                error_detail["reason"] = "Service rate-limited"
                error_detail["remediation"] = "Wait before retrying, may indicate service overload"
            elif e.status_code == 401:
                error_detail["reason"] = "Unauthorized - API key likely invalid or expired"
                error_detail["remediation"] = "Verify DEEPTHOUGHT_API_KEY environment variable"
            elif e.status_code == 403:
                error_detail["reason"] = "Forbidden - insufficient permissions"
                error_detail["remediation"] = "Check API key permissions or account status"
            elif e.status_code == 404:
                error_detail["reason"] = "Model not found or endpoint not available"
                error_detail["remediation"] = "Verify model name and base_url are correct"
            elif e.status_code >= 500:
                error_detail["reason"] = "Server error - service may be down"
                error_detail["remediation"] = "Check service status, may be temporary outage"
            
            if detailed:
                error_detail["diagnostics"] = diagnostics
            return error_detail
            
        except openai.APIConnectionError as e:
            error_detail = {
                "status": "connection_error",
                "error": str(e)[:100],
                "error_type": "api_connection_error",
                "remediation": "Check network connectivity and firewall rules",
            }
            if detailed:
                error_detail["diagnostics"] = diagnostics
            return error_detail
            
        except TimeoutError as e:
            error_detail = {
                "status": "timeout",
                "error": "Request timed out after 5 seconds",
                "error_type": "timeout",
                "remediation": "Service may be slow or unresponsive. Check service health and network latency.",
            }
            # Include timeout diagnostics if available
            if "timeout_details" not in diagnostics and detailed:
                timeout_diag = self._diagnose_timeout_details(base_url)
                diagnostics["timeout_details"] = timeout_diag
            if detailed:
                error_detail["diagnostics"] = diagnostics
            return error_detail
            
        except ImportError:
            return {
                "status": "warning",
                "note": "openai module not available",
                "diagnostics": diagnostics if detailed else {},
            }
        except Exception as e:
            error_detail = {
                "status": "error",
                "error": str(e)[:100],
                "error_type": type(e).__name__,
            }
            if detailed:
                error_detail["diagnostics"] = diagnostics
            return error_detail
    
    def liveness_check(self) -> Dict[str, Any]:
        """Fast check: is process alive?"""
        return {
            "status": "alive",
            "timestamp": datetime.now().isoformat(),
        }
    
    def readiness_check(self, include_lvm: bool = True, detailed_lvm: bool = False) -> Dict[str, Any]:
        """Complete health check with caching."""
        cache_key = f"readiness_{include_lvm}_{detailed_lvm}"
        cached = self.get_cached(cache_key)
        if cached is not None:
            cached["cached"] = True
            cached["from_cache"] = True
            return cached
        
        result = {
            "status": "ready",
            "checks": {},
            "timestamp": datetime.now().isoformat(),
            "cached": False,
        }
        
        # Environment (critical)
        env_check = self.check_environment()
        result["checks"]["environment"] = env_check
        if env_check["status"] != "ok":
            result["status"] = "not_ready"
        
        # GCS (critical)
        gcs_check = self.check_gcs_connectivity()
        result["checks"]["gcs"] = gcs_check
        if gcs_check["status"] != "ok":
            result["status"] = "not_ready"
        
        # LLM (warning if fails)
        if include_lvm:
            llm_check = self.check_lvm_connectivity(detailed=detailed_lvm)
            result["checks"]["lvm"] = llm_check
            if llm_check["status"] not in ("ok", "warning"):
                result["status"] = "degraded"
        
        self.set_cache(cache_key, result)
        return result


# Module-level singleton
_health_checker: HealthChecker | None = None

def get_health_checker() -> HealthChecker:
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker(cache_ttl_seconds=30)
    return _health_checker


def register_health_routes(app):
    """Register health check routes with Flask app."""
    
    @app.route("/healthz", methods=["GET"])
    def healthz():
        """Liveness check - always 200 if process running."""
        checker = get_health_checker()
        result = checker.liveness_check()
        return result, 200
    
    @app.route("/health", methods=["GET"])
    def health():
        """Readiness check - includes LLM test (cached)."""
        try:
            checker = get_health_checker()
            result = checker.readiness_check(include_lvm=True, detailed_lvm=False)
            status_code = 200 if result["status"] in ("ready", "degraded") else 503
            return result, status_code
        except Exception as e:
            logger.error(f"Health check error: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)[:100],
                "timestamp": datetime.now().isoformat(),
            }, 503
    
    @app.route("/health/full", methods=["GET"])
    def health_full():
        """Fresh diagnostic check - no caching, with detailed LLM diagnostics."""
        try:
            temp_checker = HealthChecker(cache_ttl_seconds=0)
            result = temp_checker.readiness_check(include_lvm=True, detailed_lvm=True)
            status_code = 200 if result["status"] in ("ready", "degraded") else 503
            return result, status_code
        except Exception as e:
            logger.error(f"Full health check error: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)[:100],
                "timestamp": datetime.now().isoformat(),
            }, 503
    
    @app.route("/health/diagnose", methods=["GET"])
    def health_diagnose():
        """Detailed LLM diagnostics without caching - best for troubleshooting."""
        try:
            temp_checker = HealthChecker(cache_ttl_seconds=0)
            result = temp_checker.check_lvm_connectivity(detailed=True)
            status_code = 200 if result["status"] == "ok" else 503
            return result, status_code
        except Exception as e:
            logger.error(f"LLM diagnostic check error: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)[:100],
                "timestamp": datetime.now().isoformat(),
            }, 503
    
    logger.info("Health check routes registered: /healthz, /health, /health/full, /health/diagnose")