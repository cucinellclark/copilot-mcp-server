#!/usr/bin/env python3
"""
Copilot MCP Server - HTTP Mode

This server provides MCP tools for python_code and rag_database functionality.
"""

from fastmcp import FastMCP
from tools.python_code_tools import register_python_code_tools
from common.token_provider import TokenProvider
from common.auth import BvbrcOAuthProvider
from starlette.responses import JSONResponse
import json
import sys
import os

# Load configuration (optional - adjust path as needed)
config_path = os.path.join(os.path.dirname(__file__), "config", "config.json")
try:
    with open(config_path, "r") as f:
        config = json.load(f)
except FileNotFoundError:
    print("Warning: config/config.json not found, using defaults", file=sys.stderr)
    config = {}

# Get configuration values
port = int(os.environ.get("PORT", config.get("port", 12011)))
mcp_url = config.get("mcp_url", "127.0.0.1")
python_code_config = config.get("python_code", {})
rag_database_config = config.get("rag_database", {})

# OAuth configuration
authentication_url = config.get("authentication_url", "https://user.patricbrc.org/authenticate")
openid_config_url = config.get("openid_config_url", "https://dev-7.bv-brc.org")

# Publicly reachable server URL for discovery and metadata
# Use PUBLIC_BASE_URL env var if set, otherwise construct from openid_config_url
server_url = os.environ.get("PUBLIC_BASE_URL") or openid_config_url

# Initialize token provider for HTTP mode
token_provider = TokenProvider(mode="http")

# Initialize OAuth provider
oauth = BvbrcOAuthProvider(
    base_url=server_url,  # This is the MCP server URL, not the data API URL
    openid_config_url=openid_config_url,
    authentication_url=authentication_url,
)

# Create FastMCP server with auth provider so /mcp is protected by FastMCP
mcp = FastMCP("Copilot MCP Server", auth=oauth)

# Register all tools with configuration and token provider
print("Registering python_code tools...", file=sys.stderr)
register_python_code_tools(mcp, python_code_config, token_provider)

# Add health check tool
@mcp.tool()
def health_check() -> str:
    """Health check endpoint"""
    return '{"status": "healthy", "service": "copilot-mcp", "mode": "http"}'

# Add OAuth2 endpoints
@mcp.custom_route("/mcp/.well-known/openid-configuration", methods=["GET"])
async def openid_configuration_route(request) -> JSONResponse:
    """
    Serves the OIDC discovery document that ChatGPT expects.
    """
    return await oauth.openid_configuration(request)

# OAuth Authorization Server metadata (well-known)
# Per RFC 8414 section 3, if issuer is "https://example.com/issuer1",
# metadata is at "https://example.com/.well-known/oauth-authorization-server/issuer1"
# So for issuer {server_url}/mcp, metadata should be at /.well-known/oauth-authorization-server/mcp
@mcp.custom_route("/.well-known/oauth-authorization-server/mcp", methods=["GET"])
async def oauth_as_metadata(request) -> JSONResponse:
    issuer = f"{server_url}/mcp"
    return JSONResponse({
        "issuer": issuer,
        "authorization_endpoint": f"{issuer}/oauth2/authorize",
        "token_endpoint": f"{issuer}/oauth2/token",
        "registration_endpoint": f"{issuer}/oauth2/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "token_endpoint_auth_methods_supported": ["none", "client_secret_post"],
        "code_challenge_methods_supported": ["S256"],
        "scopes_supported": ["profile", "token"],
    })

@mcp.custom_route("/mcp/oauth2/register", methods=["POST"])
async def oauth2_register_route(request) -> JSONResponse:
    """
    Registers a new client with the OAuth2 server.
    Implements RFC 7591 OAuth 2.0 Dynamic Client Registration.
    """
    return await oauth.oauth2_register(request)

@mcp.custom_route("/mcp/oauth2/authorize", methods=["GET"])
async def oauth2_authorize_route(request):
    """
    Authorization endpoint - displays login page for user authentication.
    This is where ChatGPT redirects the user to log in.
    """
    return await oauth.oauth2_authorize(request)

@mcp.custom_route("/mcp/oauth2/login", methods=["POST"])
async def oauth2_login_route(request):
    """
    Handles the login form submission.
    Authenticates the user and generates an authorization code.
    Redirects back to ChatGPT's callback URL with the code.
    """
    return await oauth.oauth2_login(request)

@mcp.custom_route("/mcp/oauth2/token", methods=["POST"])
async def oauth2_token_route(request):
    """
    Handles the token request.
    Exchanges an authorization code for an access token.
    Retrieves the stored user token using the authorization code.
    """
    return await oauth.oauth2_token(request)

def main() -> int:
    """Main entry point for the Copilot MCP Server in HTTP mode."""
    print(f"Starting Copilot MCP Server on port {port}...", file=sys.stderr)
    print(f"  - Server URL: http://{mcp_url}:{port}", file=sys.stderr)
    
    try:
        # Run in HTTP mode
        mcp.run(transport="http", host=mcp_url, port=port)
    except KeyboardInterrupt:
        print("Server stopped.", file=sys.stderr)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

