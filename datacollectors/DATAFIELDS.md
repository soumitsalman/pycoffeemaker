# RSS Feed Field Reference (feedparser)

This document summarizes which fields in an RSS feed (as parsed by feedparser) typically contain common news/article metadata. Additional notes are provided to guide processing logic for fields that are fuzzy, rare, optional, or overlapping.

## Article/Entry-Level Fields

| Info Type                | Field(s) (entry)                        | Notes |
|--------------------------|------------------------------------------|-------|
| Title                    | `entry.title`                            | Always present in well-formed feeds. Use as the primary title. |
| Summary/Excerpt          | `entry.summary`, `entry.description`     | Often same as summary. Use `entry.summary` first, fallback to `entry.description`. |
| Full Content             | `entry.content[0]['value']`, `entry.content`, `entry['content:encoded']`, `entry['dc_content']` | May require fallback logic. Check for `entry.content` first, then try other fields. |
| Featured/Top Image       | `entry.media_content[0]['url']`, `entry.media_thumbnail[0]['url']`, `entry.links` (where type contains 'image') | Not always present. Use `media_content` first, fallback to `media_thumbnail` or `links`. |
| List of Images           | `entry.media_content`, `entry.media_thumbnail`, `entry.links` (filter for images) | Iterate through all possible fields to extract image URLs. |
| Tags/Keywords            | `entry.tags`                             | List of dicts, each with `term`. Check for `tags` and handle missing or empty lists gracefully. |
| Language                 | `entry.language` (rare), fallback to `feed.language` | Usually only at feed level. Use `feed.language` as a fallback. |
| Country                  | `entry['geo_country']` (rare, custom), infer from feed or URL | Not standardized. May require heuristics or custom logic. |
| Content Type/Kind        | (not standard) infer from context, or custom fields | Use context or custom fields to infer type (e.g., blog, news, article). |
| Author Name              | `entry.author`, `entry.author_detail['name']` | Use `author_detail['name']` if available, fallback to `author`. |
| Author Email             | `entry.author_detail['email']`           | Optional. Check `author_detail` for email. |
| Author Social Media      | (not standard) sometimes in `entry.author_detail['href']` | Rare. May require custom parsing. |
| Publisher/Source Name    | `entry.source.title`, `feed.title`       | Use `entry.source.title` for entry-level, fallback to `feed.title`. |
| Source Base URL          | `entry.source.href`, `feed.link`         | Use `entry.source.href` for entry-level, fallback to `feed.link`. |
| Favicon                  | `feed.icon`, `feed.image.href`           | Check `feed.icon` first, fallback to `feed.image.href`. |
| Source Description       | `feed.subtitle`, `feed.description`      | Use `feed.subtitle` first, fallback to `feed.description`. |
| Published Date           | `entry.published`, `entry.published_parsed`, `entry.updated`, `entry.updated_parsed` | Use `published_parsed` or `updated_parsed` for datetime objects. |
| Comments (count/link)    | `entry.comments`, `entry.wfw_commentrss`, `entry.slash_comments` | Not always present. Check all fields for comments. |
| Additional Info          | Custom fields, e.g. `entry['custom_field']` | Varies by feed. Handle custom fields dynamically. |

## Feed-Level Fields

| Info Type                | Field(s) (feed)                          | Notes |
|--------------------------|------------------------------------------|-------|
| Feed Title               | `feed.title`                             | Always present in well-formed feeds. |
| Feed Language            | `feed.language`                          | Use as the primary language field. |
| Feed Country             | (not standard) sometimes custom, e.g. `feed['geo_country']` | Rare. May require heuristics or custom logic. |
| Feed Author              | `feed.author`, `feed.author_detail`      | Use `author_detail` for structured data. |
| Feed Publisher           | `feed.title`                             | Use as the primary publisher name. |
| Feed Base URL            | `feed.link`                              | Always present in well-formed feeds. |
| Feed Favicon             | `feed.icon`, `feed.image.href`           | Check `feed.icon` first, fallback to `feed.image.href`. |
| Feed Description         | `feed.subtitle`, `feed.description`      | Use `feed.subtitle` first, fallback to `feed.description`. |
| Feed Published/Updated   | `feed.published`, `feed.updated`         | Use `published` or `updated` for feed-level timestamps. |

## Example: Accessing Fields

```python
feed = feedparser.parse(rss_url)
for entry in feed.entries:
    title = entry.get('title')
    summary = entry.get('summary') or entry.get('description')
    content = entry.get('content', [{}])[0].get('value') if 'content' in entry else None
    images = [img.get('url') for img in entry.get('media_content', [])]
    tags = [tag['term'] for tag in entry.get('tags', [])]
    author = entry.get('author')
    author_email = entry.get('author_detail', {}).get('email')
    published = entry.get('published')
    comments = entry.get('comments')
    # Handle fuzzy or optional fields
    language = entry.get('language') or feed.feed.get('language')
    country = entry.get('geo_country') or infer_country_from_url(feed.feed.get('link'))
    # ...etc
```