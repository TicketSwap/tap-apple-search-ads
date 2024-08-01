"""AppleSearchAds Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class AppleSearchAdsAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for AppleSearchAds."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the AutomaticTestTap API.

        Returns:
            A dict with the request body
        """
        # TODO: Define the request body needed for the API.
        return {
            "scope": self.oauth_scopes,
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "grant_type": "client_credentials",
        }

    @classmethod
    def create_for_stream(cls, stream) -> AppleSearchAdsAuthenticator:  # noqa: ANN001
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            auth_endpoint="https://appleid.apple.com/auth/oauth2/token",
            oauth_scopes="searchadsorg",
            default_expiration=3600,
            oauth_headers={
                "Host": "appleid.apple.com",
                "Content-Type": "application/x-www-form-urlencoded",
            }
        )
