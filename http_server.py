#!/usr/bin/env python3
"""
Copilot MCP Server - HTTP Mode

This server provides MCP tools for python_code and rag_database functionality.
"""

from fastmcp import FastMCP
from fastmcp.utilities.logging import get_logger, configure_logging
from tools.python_code_tools import register_python_code_tools
from tools.rag_database_tools import register_rag_database_tools
from tools.file_utilities_tools import register_file_utilities_tools
from common.token_provider import TokenProvider
from common.auth import BvbrcOAuthProvider
from common.config import get_config
from starlette.responses import JSONResponse
import sys

# Configure logging
configure_logging(level='INFO')
logger = get_logger(__name__)

# Load configuration
config = get_config()

# Get configuration values
port = config.port
mcp_url = config.mcp_url
python_code_config = config.python_code
rag_database_config = config.rag_database
file_utilities_config = config.file_utilities

# OAuth configuration
authentication_url = config.authentication_url
openid_config_url = config.openid_config_url

# Publicly reachable server URL for discovery and metadata
server_url = config.server_url

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
logger.info("Registering python_code tools...")
register_python_code_tools(mcp, python_code_config, token_provider)
register_rag_database_tools(mcp, rag_database_config)
register_file_utilities_tools(mcp, file_utilities_config)
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
    logger.info(f"Starting Copilot MCP Server on port {port}...")
    logger.info(f"  - Server URL: http://{mcp_url}:{port}")
    
    try:
        # Run in HTTP mode
        mcp.run(transport="http", host=mcp_url, port=port)
    except KeyboardInterrupt:
        logger.info("Server stopped.")
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

