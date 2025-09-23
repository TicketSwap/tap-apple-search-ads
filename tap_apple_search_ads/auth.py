"""AppleSearchAds Authentication."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from singer_sdk.authenticators import OAuthAuthenticator

if TYPE_CHECKING:
    from singer_sdk.streams import RESTStream


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class AppleSearchAdsAuthenticator(OAuthAuthenticator):
    """Authenticator class for AppleSearchAds."""

    def __init__(
        self,
        org_id: str,
        is_partitioned: bool,  # noqa: FBT001
        stream: RESTStream,
        auth_endpoint: str,
        oauth_scopes: str,
        default_expiration: int,
        oauth_headers: dict,
    ) -> None:
        """Create a new authenticator instance.

        Args:
            org_id: The organization ID.
            is_partitioned: Whether to use partitioning for the requests.
            kwargs: The keyword arguments to pass to the parent constructor.
        """
        self.org_id = org_id
        self.is_partitioned = is_partitioned
        super().__init__(
            stream=stream,
            auth_endpoint=auth_endpoint,
            oauth_scopes=oauth_scopes,
            default_expiration=default_expiration,
            oauth_headers=oauth_headers,
        )

    @property
    def client_id(self) -> str | None:
        """Return the client ID.

        Returns:
            The client ID.
        """
        if self.is_partitioned:
            return os.environ[f"TAP_APPLE_SEARCH_ADS_CLIENT_ID__{self.org_id}"]
        return super().client_id

    @property
    def client_secret(self) -> str | None:
        """Return the client secret.

        Returns:
            The client secret.
        """
        if self.is_partitioned:
            return os.environ[f"TAP_APPLE_SEARCH_ADS_CLIENT_SECRET__{self.org_id}"]
        return super().client_secret

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the AutomaticTestTap API.

        Returns:
            A dict with the request body
        """
        return {
            "scope": self.oauth_scopes,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }
