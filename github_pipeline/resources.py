from typing import Any, Generator, List
from urllib.parse import urljoin

import requests
from dagster import ConfigurableResource, get_dagster_logger


class GitHubAPIResource(ConfigurableResource):
    """Custom Dagster resource for the GitHub REST API.

    Args:
        - github_token (str | None, optional): \
            GitHub token for authentication. If no token is set, the API calls will be without authentication.
        - host (str, optional): \
            Host address of the Contentful Management API. Defaults to 'https://api.github.com'.
    """

    github_token: str | None = None
    """GitHub token for authentication. If no token is set, the API calls will be without authentication."""

    host: str = 'https://api.github.com'
    """Host of the GitHub REST API."""

    def execute_request(
        self,
        method: str,
        path: str,
        params: dict | list[tuple] | None = None,
        json: Any | None = None,
    ) -> requests.Response:
        """Execute a request to the GitHub REST API.

        Args:
            - method (str): \
                HTTP method for the API call, e.g. 'GET'.
            - path (str): \
                Path of the endpoint.
            - params (dict | list[tuple], optional): \
                Dictionary or list of tuples to send as query parameters in the request.
            - json (Any, optional): \
                A JSON serializable Python object to send in the body of the request.

        Returns:
            - requests.Response: \
                Response object of the API call.

        Raises:
            - requests.HTTPError: \
                When HTTP 4xx or 5xx response is received.
        """
        if params is None:
            params = {}
        default_params = {'apiVersion': '2022-11-28'}
        # passed parameters win over the default parameters
        params = {**default_params, **params}

        headers = {'Accept': 'application/vnd.github+json'}
        if self.github_token:
            headers['Authorization'] = f'Bearer {self.github_token}'

        try:
            response = requests.request(
                method=method,
                url=urljoin(self.host, path),
                params=params,
                headers=headers,
                json=json,
            )
            get_dagster_logger().info(f'Call {method}: {response.url}')

            response.raise_for_status()

        except requests.exceptions.HTTPError as err:
            get_dagster_logger().exception(f'{err!r} - {response.text}')

        return response

    def get_repository(self, owner: str, repo: str) -> dict[str, Any]:
        """Get metadata about a GitHub repository.
        Docs: https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#get-a-repository

        Args:
            - owner (str): \
                The account owner of the repository. The name is not case sensitive.
            - repo (str): \
                The name of the repository without the `.git` extension. The name is not case sensitive.

        Returns:
            - dict[str, Any]: \
                The metadata for the repository.
        """
        path = f'/repos/{owner}/{repo}'
        response = self.execute_request(method='GET', path=path)
        payload = response.json()

        return payload

    def get_all_releases(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """Get all releases for a GitHub repository, handling pagination.
        Docs: https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#list-releases

        Args:
            - owner (str): \
                The account owner of the repository. The name is not case sensitive.
            - repo (str): \
                The name of the repository without the `.git` extension. The name is not case sensitive.

        Returns:
            - list[dict[str, Any]]: \
                A list of all releases for the repository, where each element is a release object.
        """
        path = f'/repos/{owner}/{repo}/releases'
        releases = []
        page = 1
        per_page = 100

        while True:
            params = {'page': page, 'per_page': per_page}
            response = self.execute_request(method='GET', path=path, params=params)
            data = response.json()
            if not data:
                break
            releases.extend(data)
            page += 1

        return releases

    def get_all_issues(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """Get all issues for a GitHub repository, handling pagination.
        Docs: https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#list-repository-issues

        Args:
            - owner (str): \
                The account owner of the repository. The name is not case sensitive.
            - repo (str): \
                The name of the repository without the `.git` extension. The name is not case sensitive.

        Returns:
            - list[dict[str, Any]]: \
                A list of all issues for the repository, where each element is an issue object.
        """
        path = f'/repos/{owner}/{repo}/issues'
        issues = []
        page = 1
        per_page = 100

        while True:
            params = {'page': page, 'per_page': per_page, 'state': 'all'}
            response = self.execute_request(method='GET', path=path, params=params)
            data = response.json()
            if not data:
                break
            issues.extend(data)
            page += 1

        return issues
