"""
Tests for the YouTube client.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from youtube_collector import YouTubeClient, ConfigError


class TestYouTubeClientInit:
    """Test YouTubeClient initialization."""

    def test_init_with_api_key(self):
        """Test initialization with direct API key."""
        client = YouTubeClient(api_key="test_api_key_12345678901234567890")
        assert client.api_key == "test_api_key_12345678901234567890"

    @patch.dict('os.environ', {'YOUTUBE_API_KEY': 'env_api_key_12345678901234567890'})
    def test_init_with_env_var(self):
        """Test initialization with environment variable."""
        client = YouTubeClient()
        assert client.api_key == "env_api_key_12345678901234567890"

    def test_init_without_api_key_raises_error(self):
        """Test that missing API key raises ConfigError."""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ConfigError):
                YouTubeClient()


class TestFetchVideosByPublishDate:
    """Test fetch_videos_by_publish_date method."""

    @pytest.fixture
    def mock_youtube_client(self):
        """Create a mocked YouTube client."""
        with patch('youtube_collector.client.build') as mock_build:
            mock_youtube = MagicMock()
            mock_build.return_value = mock_youtube

            client = YouTubeClient(api_key="test_key_12345678901234567890")
            client.youtube = mock_youtube

            yield client, mock_youtube

    def test_fetch_videos_success(self, mock_youtube_client):
        """Test successful video fetching."""
        client, mock_youtube = mock_youtube_client

        # Mock search response
        mock_search = MagicMock()
        mock_search.list().execute.return_value = {
            'items': [
                {'id': {'videoId': 'video1'}},
                {'id': {'videoId': 'video2'}},
            ]
        }

        # Mock videos response
        mock_videos = MagicMock()
        mock_videos.list().execute.return_value = {
            'items': [
                {
                    'id': 'video1',
                    'snippet': {
                        'title': 'Test Video 1',
                        'categoryId': '10',
                        'publishedAt': '2024-01-01T00:00:00Z',
                        'thumbnails': {'high': {'url': 'http://example.com/thumb1.jpg'}},
                        'channelId': 'channel1',
                        'channelTitle': 'Test Channel',
                        'description': 'Test description'
                    },
                    'statistics': {
                        'viewCount': '1000',
                        'likeCount': '100',
                        'commentCount': '10'
                    }
                }
            ]
        }

        # Mock channels response
        mock_channels = MagicMock()
        mock_channels.list().execute.return_value = {
            'items': [
                {
                    'id': 'channel1',
                    'statistics': {
                        'subscriberCount': '50000'
                    }
                }
            ]
        }

        mock_youtube.search.return_value = mock_search
        mock_youtube.videos.return_value = mock_videos
        mock_youtube.channels.return_value = mock_channels

        # Execute
        videos = client.fetch_videos_by_publish_date(days_ago=7)

        # Assert
        assert len(videos) > 0
        assert videos[0]['video_id'] == 'video1'
        assert videos[0]['title'] == 'Test Video 1'
        assert videos[0]['views'] == 1000
        assert videos[0]['likes'] == 100
        assert videos[0]['channel_subscribers'] == 50000

    def test_fetch_videos_with_custom_categories(self, mock_youtube_client):
        """Test fetching with custom categories."""
        client, mock_youtube = mock_youtube_client

        mock_search = MagicMock()
        mock_search.list().execute.return_value = {'items': []}
        mock_youtube.search.return_value = mock_search

        mock_channels = MagicMock()
        mock_youtube.channels.return_value = mock_channels

        # Execute with custom categories
        client.fetch_videos_by_publish_date(
            days_ago=30,
            categories=['10', '20']
        )

        # Verify it only called for 2 categories
        assert mock_search.list().execute.call_count == 2

    def test_fetch_videos_handles_api_error(self, mock_youtube_client):
        """Test that API errors are handled gracefully."""
        client, mock_youtube = mock_youtube_client

        mock_search = MagicMock()
        mock_search.list().execute.side_effect = Exception("API Error")
        mock_youtube.search.return_value = mock_search

        mock_channels = MagicMock()
        mock_youtube.channels.return_value = mock_channels

        # Should not raise, should return empty list
        videos = client.fetch_videos_by_publish_date(days_ago=7)
        assert videos == []


class TestDownloadThumbnail:
    """Test download_thumbnail method."""

    @pytest.fixture
    def client(self):
        """Create a YouTube client."""
        return YouTubeClient(api_key="test_key_12345678901234567890")

    @patch('youtube_collector.client.requests.get')
    @patch('youtube_collector.client.Path.mkdir')
    @patch('builtins.open', create=True)
    def test_download_thumbnail_success(self, mock_open, mock_mkdir, mock_get, client, tmp_path):
        """Test successful thumbnail download."""
        # Mock response
        mock_response = Mock()
        mock_response.content = b'fake_image_data'
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Execute
        result = client.download_thumbnail(
            thumbnail_url='http://example.com/thumb.jpg',
            video_id='test123',
            output_dir=str(tmp_path)
        )

        # Assert
        assert 'test123.jpg' in result
        mock_get.assert_called_once()

    @patch('youtube_collector.client.requests.get')
    def test_download_thumbnail_request_error(self, mock_get, client):
        """Test download with request error."""
        import requests
        mock_get.side_effect = requests.RequestException("Network error")

        with pytest.raises(requests.RequestException):
            client.download_thumbnail(
                thumbnail_url='http://example.com/thumb.jpg',
                video_id='test123'
            )


class TestDownloadThumbnailsBulk:
    """Test download_thumbnails_bulk method."""

    @pytest.fixture
    def client(self):
        """Create a YouTube client."""
        return YouTubeClient(api_key="test_key_12345678901234567890")

    @patch.object(YouTubeClient, 'download_thumbnail')
    def test_bulk_download_success(self, mock_download, client):
        """Test bulk thumbnail download."""
        mock_download.return_value = '/path/to/thumbnail.jpg'

        videos = [
            {'video_id': 'video1', 'thumbnail_url': 'http://example.com/1.jpg'},
            {'video_id': 'video2', 'thumbnail_url': 'http://example.com/2.jpg'},
        ]

        results = client.download_thumbnails_bulk(videos)

        assert len(results) == 2
        assert 'video1' in results
        assert 'video2' in results
        assert mock_download.call_count == 2

    @patch.object(YouTubeClient, 'download_thumbnail')
    def test_bulk_download_with_failures(self, mock_download, client):
        """Test bulk download handles individual failures."""
        def side_effect(url, video_id, output_dir=None):
            if video_id == 'video2':
                raise Exception("Download failed")
            return f'/path/to/{video_id}.jpg'

        mock_download.side_effect = side_effect

        videos = [
            {'video_id': 'video1', 'thumbnail_url': 'http://example.com/1.jpg'},
            {'video_id': 'video2', 'thumbnail_url': 'http://example.com/2.jpg'},
            {'video_id': 'video3', 'thumbnail_url': 'http://example.com/3.jpg'},
        ]

        results = client.download_thumbnails_bulk(videos)

        # Should succeed for video1 and video3, fail for video2
        assert len(results) == 2
        assert 'video1' in results
        assert 'video2' not in results
        assert 'video3' in results
