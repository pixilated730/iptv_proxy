//proxy
const fs = require('fs');
const http = require('http');
const https = require('https');
const url = require('url');
const zlib = require('zlib');
let Redis;

if (process.env.REDIS_URL) {
  Redis = require("ioredis");
}

let sessionTokens = {};
let lastCheckedTimestamps = {};
const storage = initializeStorage();

function logToFile(message) {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] ${message}\n`;
  fs.appendFileSync('logs.txt', logEntry);
}

http.createServer(async (req, res) => {
  try {
    setCorsHeaders(res);

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const parsedUrl = url.parse(req.url, true);
    const pathname = parsedUrl.pathname;
    const query = parsedUrl.query;

    logToFile(`Incoming request: ${req.url}`);
    logToFile('Query parameters: ' + JSON.stringify(query));

    if (pathname === '/' && !parsedUrl.search) {
      const html = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta name="robots" content="noindex, nofollow">
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>M3U Playlist Proxy</title>
    <style>
    body{
        color: #626262;
    }
    form {
        max-width: 600px;
        margin: 0 auto;
        padding: 20px;
        background-color: #f7f7f7;
        border: 1px solid #ddd;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    input[type="text"] {
        width: calc(50% - 10px);
        padding: 10px;
        font-size: 16px;
        border: 1px solid #ccc;
        border-radius: 4px;
        margin-bottom: 10px;
        color: #626262;
    }
    input[type="text"]:focus {
        border-color: #007bff;
        outline: none;
        box-shadow: 0 0 5px rgba(0, 123, 255, 0.5);
    }
    button {
        padding: 10px 15px;
        font-size: 16px;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        margin-top: 10px;
        width: 45%;
        margin-left: 20px;
        align-content: center;
    }
    button#add-more,fetchPlaylistGroups {
        background-color: #007bff;
        color: white;
    }
    button#fetchPlaylistGroups {
        background-color: #007bff;
        color: white;
    }
    button[type="submit"] {
		background-color: #28a745;
		color: white;
		width: 94%;
		font-size: 18px;
		margin-top: 20px;
    }
    button:hover {
        opacity: 0.9;
    }
    button:focus {
        outline: none;
    }
    select {
      width: 100%;
      font-size: 16px;
      padding: 10px;
      font-size: 16px;
      border: 1px solid #ccc;
      border-radius: 4px;
      margin-bottom: 20px;
      background-color: #fff;
      color: #626262;
    }
    textarea {
        width: 50%;
        max-width: 600px;
        margin: 0 auto;
        display: block;
        padding: 10px;
        font-size: 16px;
        border: 1px solid #ccc;
        border-radius: 4px;
        margin-top: 10px;
        resize: none;
        color: #626262;
    }
    .header-pair {
        display: flex;
        justify-content: space-between;
        margin-bottom: 10px;
    }
    h3, h4 {
        font-size: 24px;
        font-weight: bold;
        margin-bottom: 20px;
        color: #626262;
    }
    .container {
        max-width: 600px;
        margin: 0 auto;
    }
    .epg_container {
        float: right;
        margin-right: 10px;
        position: relative;
        top: -10px;
    }
    .footer {
        max-width: 600px;
        margin: 0 auto;
        padding-top: 20px;
        padding-bottom: 10px;
        text-align: center;
    }
    .group-checkbox-container {
        max-width: 600px;
        margin: 20px auto;
        padding: 10px;
        border: 1px solid #ddd;
        border-radius: 8px;
        height: 200px;
        overflow-y: auto;
        background-color: #f7f7f7;
    }
    .group-checkbox-container label {
        display: block;
        margin-bottom: 5px;
    }
    @media (max-width: 600px) {
        input[type="text"] {
            width: 100%;
            margin-bottom: 10px;
        }
        .header-pair {
            flex-direction: column;
        }
    }
    pre {
      background-color: #eaeaea;
      color: #000;
      padding-top: 15px;
      padding-left: 15px;
    }
    #help-overlay {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background-color: rgba(0, 0, 0, 0.8);
        color: #fff;
        display: none;
        justify-content: center;
        align-items: center;
        z-index: 1000;
    }
    #help-content {
        position: relative;
        background-color: #fff;
        color: #333;
        padding: 20px;
        border-radius: 8px;
        max-width: 80%;
        width: 60%;
        max-height: 70%;
        overflow: scroll;
    }
    #close-help {
      position: absolute;
      top: 5px;
      right: 20px;
      background: none;
      border: none;
      font-size: 34px;
      font-weight: bold;
      color: #ff4e4e;
      cursor: pointer;
    }
    #close-help:hover {
        opacity: 0.7;
    }
    </style>
</head>
<body>
    <center><h3>M3U Playlist Proxy</h3></center>
    <div class="container">
        <p>Use the form below to generate a playlist with the necessary headers. For example, if the server requires a "Referer: http://example.com" header to allow streaming, you would enter "Referer" as the Header Name and "http://example.com" as the Header Value. <a href="#" id="help-button">More Info.</a></p>
    </div>

    <form id="headerForm">
    <div>
        <label for="playlistUrl">Playlist URL (use comma for multiple URLs):</label>
        <div class="epg_container">
            <input type="checkbox" id="epgMerging" checked><label style="font-size:13px;margin-left:2px;">Merge epg's</label>
        </div>
        <input type="text" id="playlistUrl" name="playlistUrl" placeholder="Enter playlist URL" style="width: 96%; margin-bottom: 20px;">
    </div>


	 <label id="label-checkbox-container" style="display:none;margin-bottom:-15px;">Select Groups:</label>

    <div class="group-checkbox-container" id="groupCheckboxContainer" style="display:none;">
        <label><input type="checkbox" id="checkUncheckAll" value="all"> Check/Uncheck All</label>
    </div>

    <div class="header-pair">
        <input type="text" name="headerName" placeholder="Header Name">
        <input type="text" name="headerValue" placeholder="Header Value">
    </div>


    <button type="button" id="add-more">Add More Headers</button>
	<button type="button" id="fetchPlaylistGroups">Choose Groups</button>
    <button type="submit">Generate Playlist URL</button>
    </form>

    <center><h4>Generated Playlist URL:</h4></center>

    <textarea id="result" rows="4" cols="80" readonly></textarea>
    <div class="container">
        <p>Once the URL has been generated, you can use a URL shortener like <a href="https://tinyurl.com" target="_blank">TinyURL.com</a> to shorten it. This will make it much easier to add the URL as a M3U playlist within your IPTV application.</p>
        <div class="container" id="firewall-warning"></div>
    </div>

    <div class="footer">Created by <a href="https://github.com/dtankdempse/m3u-playlist-proxy">Tank Dempse</a></div>

    <div id="help-overlay">
        <div id="help-content">
            <span id="close-help">&times;</span>

    <h4>Adding Playlist URL(s)</h4>

    <p>When adding multiple playlist URLs, separate them with a comma. This will merge all playlists into a single combined list. If multiple EPGs are defined in the playlists using the <code>url-tvg</code> tag, they will also be merged into a single EPG file.</p>

    <strong>Example:</strong>
    <pre>
http://example.com/playlist1.m3u8,http://example.com/playlist2.m3u8,http://example.com/playlist3.m3u8
    </pre>

    <p>The example above will merge three playlists into a single playlist.</p>

    <h4>Merge EPGs</h4>

    <p>When checked, this option combines EPG sources (specified by <code>tvg-url</code>) into a single EPG file if more than one playlist is used. If only one playlist is used, the <code>tvg-url</code> will remain untouched. This provides a consolidated channel guide across merged playlists. Leaving this unchecked removes all <code>tvg-url</code> tags when multiple sources are detected, helping to reduce bandwidth usage.</p>

	<h4>Select Groups</h4>

    <p>The Select Groups can be used to filter out channels from the playlist based on the group titles. First click on the Choose Groups button to fetch the playlist, the grpups will then be listed. Check the box next to each group you want to include in the playlist; any unchecked boxes will be excluded from the playlist.</p>

    <h4>Headers in the Playlist</h4>
    <p>If specific headers for applications like VLC, TiviMate, or Kodi are included within the playlist, MPP will use those headers to proxy the individual streams. This means the headers embedded in the playlist itself will be utilized directly when accessing a particular stream.</p>

    <h4>Headers in the Header Fields</h4>
    <p>If no headers are present within the playlist for a given stream, MPP will fall back to using the headers specified in the "Header Name / Value" fields of the form. This allows for a default set of headers to be used when the playlist lacks specific instructions.</p>

    <h4>No Headers Set</h4>
    <p>If neither the playlist nor the "Header Name / Value" fields provide any headers, the streams will be accessed without any headers, essentially passing through unmodified, which means they won't be explicitly proxied by MPP.</p>

    <h4>Priority System for Headers</h4>
    <p>Overall, MPP outlines a priority system for using headers:</p>
    <ol>
        <li>Headers embedded in the playlist.</li>
        <li>Headers provided via form input.</li>
        <li>No headers at all.</li>
    </ol>

    <h4>Supported Formats</h4>
    <p>The following are supported formats for specifying headers within a playlist:</p>

    <strong>Format Example 1:</strong>
    <pre>
#EXTINF:-1,Channel Name
http://example.com/playlist.m3u8|Referer="http://example.com"|User-Agent="VLC/3.0.20 LibVLC/3.0.20"
    </pre>

    <strong>Format Example 2:</strong>
    <pre>
#EXTINF:-1,Channel Name
http://example.com/playlist.m3u8|Referer=http://example.com|User-Agent=VLC/3.0.20 LibVLC/3.0.20
    </pre>

    <strong>Format Example 3:</strong>
    <pre>
#EXTINF:-1,Channel Name
http://example.com/playlist.m3u8|Referer=http%3A%2F%2Fexample.com&User-Agent=VLC%2F3.0.20%20LibVLC%2F3.0.20
    </pre>

    <strong>Format Example 4:</strong>
    <pre>
#EXTINF:-1,Channel Name
#EXTVLCOPT:http-referrer=http://example.com
#EXTVLCOPT:http-user-agent=VLC/3.0.20 LibVLC/3.0.20
http://example.com/playlist.m3u8
    </pre>
        </div>
    </div>

    <script>
        document.getElementById('add-more').addEventListener('click', function () {
            const headerPair = document.createElement('div');
            headerPair.classList.add('header-pair');
            headerPair.innerHTML =
                "<input type='text' name='headerName' placeholder='Header Name'>" +
                "<input type='text' name='headerValue' placeholder='Header Value'>";
            document.getElementById('headerForm').insertBefore(headerPair, document.getElementById('add-more'));
        });

        document.getElementById('checkUncheckAll').addEventListener('change', function () {
            const isChecked = this.checked;
            document.querySelectorAll('.group-checkbox').forEach(checkbox => checkbox.checked = isChecked);
        });

        document.getElementById('fetchPlaylistGroups').addEventListener('click', function (event) {
            document.getElementById('label-checkbox-container').style.display = 'block';
            event.preventDefault();

            const playlistUrl = document.getElementById('playlistUrl').value.trim();
            if (!playlistUrl) {
                alert('Please enter a Playlist URL to fetch groups.');
                return;
            }

            const urls = playlistUrl.split(',').map(url => url.trim());
            const groupTitles = new Set();

            function fetchPlaylist(url) {
                return fetch('/fetch?url=' + encodeURIComponent(url), {
                    method: 'GET'
                })
                .then(response => response.text())
                .then(data => {
                    if (data.includes('<a href="')) {
                        const redirectUrlMatch = data.match(/<a href="(.*?)"/);
                        if (redirectUrlMatch) {
                            return fetchPlaylist(redirectUrlMatch[1]);
                        }
                    }
                    const regex = /group-title="(.*?)"/gi;
                    let match;
                    while ((match = regex.exec(data)) !== null) {
                        groupTitles.add(match[1]);
                    }
                })
                .catch(error => {
                    console.error('Error fetching the playlist:', error);
                    alert('Failed to fetch the playlist. Please check the URL and try again.');
                });
            }

            Promise.all(urls.map(url => fetchPlaylist(url))).then(() => {
                const groupContainer = document.getElementById('groupCheckboxContainer');
                groupContainer.style.display = 'block';
                groupContainer.innerHTML = '<label><input type="checkbox" id="checkUncheckAll" value="all"> Check/Uncheck All</label>';
                Array.from(groupTitles).sort().forEach(group => {
                    const label = document.createElement('label');
                    label.innerHTML = '<input type="checkbox" class="group-checkbox" value="' + group + '" checked> ' + group;
                    groupContainer.appendChild(label);
                });

                document.getElementById('checkUncheckAll').addEventListener('change', function () {
                    const isChecked = this.checked;
                    document.querySelectorAll('.group-checkbox').forEach(checkbox => checkbox.checked = isChecked);
                });
            });
        });

        document.getElementById('headerForm').addEventListener('submit', function (event) {
            event.preventDefault();

            const playlistUrl = document.getElementById('playlistUrl').value.trim();
            if (!playlistUrl) {
                alert('Please enter a Playlist URL.');
                return;
            }

            let headers = [];
            const headerPairs = document.querySelectorAll('.header-pair');

            headerPairs.forEach(pair => {
                const headerName = pair.querySelector('input[name="headerName"]').value;
                const headerValue = pair.querySelector('input[name="headerValue"]').value;
                if (headerName && headerValue) {
                    headers.push(headerName + "=" + headerValue);
                }
            });

            const baseUrl = window.location.origin;
            let fullUrl = baseUrl + "/playlist?url=" + encodeURIComponent(playlistUrl);

            if (headers.length > 0) {
                const headerString = headers.join('|');
                const base64Encoded = btoa(headerString);
                const urlEncodedData = encodeURIComponent(base64Encoded);
                fullUrl += "&data=" + urlEncodedData;
            }

            const epgMergingChecked = document.getElementById('epgMerging').checked;
            if (epgMergingChecked) {
                fullUrl += "&epgMerging=true";
            }

            let excludedGroups = [];
            document.querySelectorAll('.group-checkbox').forEach(checkbox => {
                if (!checkbox.checked) {
                    excludedGroups.push(checkbox.value);
                }
            });

            if (excludedGroups.length > 0) {
                fullUrl += "&exclude=" + encodeURIComponent(excludedGroups.join(','));
            }

            document.getElementById('result').value = fullUrl;
        });

        document.addEventListener('DOMContentLoaded', function() {
            const host = window.location.hostname;
            if (host === 'localhost' || host === '127.0.0.1') {
                const warning = document.createElement('div');
                warning.classList.add('container');
                warning.style.color = '#ff4e4e';
                warning.style.fontSize = '20px';
                warning.style.textAlign = 'center';
                warning.style.fontWeight = 'bold';
                warning.innerHTML = '<p>Warning: If you are accessing this page via <code>127.0.0.1</code> or <code>localhost</code>, proxying will not work on other devices. Please load this page using your computers IP address (e.g., <code>192.168.x.x</code>) and port in order to access the playlist from other devices on your network.</p><p>How to locate ip address on <a href="https://www.youtube.com/watch?v=UAhDHXN2c6E" target="_blank">Windows</a> or <a href="https://www.youtube.com/watch?v=gaIYP4TZfHI" target="_blank">Linux</a>.</p>';
                document.body.insertBefore(warning, document.body.firstChild);
            }
        });

        document.addEventListener('DOMContentLoaded', function() {
            const port = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');
            const warningMessage =
            '<p>Also, ensure that port <strong>' + port + '</strong> is open and allowed through your Windows (<a href="https://youtu.be/zOZWlTplrcA?si=nGXrHKU4sAQsy18e&t=18" target="_blank">how to</a>) or Linux  (<a href="https://youtu.be/7c_V_3nWWbA?si=Hkd_II9myn-AkNnS&t=12" target="_blank">how to</a>) firewall settings. This will enable other devices, such as Firestick, Android, and others, to connect to the server and request the playlist through the proxy.</p>';

            document.getElementById('firewall-warning').innerHTML = warningMessage;
        });

        document.getElementById('help-button').addEventListener('click', function() {
            document.getElementById('help-overlay').style.display = 'flex';
        });

        document.getElementById('close-help').addEventListener('click', function() {
            document.getElementById('help-overlay').style.display = 'none';
        });

        document.getElementById('help-overlay').addEventListener('click', function(event) {
            if (event.target === document.getElementById('help-overlay')) {
                document.getElementById('help-overlay').style.display = 'none';
            }
        });
    </script>
</body>
</html>
`;
      setCorsHeaders(res);
      res.writeHead(200, {
        'Content-Type': 'text/html'
      });
      res.end(html);
      return;
    }

    if (parsedUrl.pathname === '/fetch') {
      const targetUrl = parsedUrl.query.url;
      if (!targetUrl) {
        setCorsHeaders(res);
        res.writeHead(400, {
          'Content-Type': 'application/json'
        });
        res.end(JSON.stringify({ error: 'Missing URL parameter' }));
        return;
      }

      try {
        const protocol = targetUrl.startsWith('https') ? https : http;

        protocol.get(targetUrl, (response) => {
          let data = '';

          response.on('data', chunk => {
            data += chunk;
          });

          response.on('end', () => {
            if (!res.headersSent) {
              setCorsHeaders(res);
              res.writeHead(200, {
                'Content-Type': 'text/plain'
              });
              res.end(data);
            }
          });
        }).on('error', (e) => {
          if (!res.headersSent) {
            setCorsHeaders(res);
            res.writeHead(500, {
              'Content-Type': 'application/json'
            });
            res.end(JSON.stringify({ error: e.message }));
          }
        });

      } catch (error) {
        logToFile('Error in /fetch: ' + error);
        if (!res.headersSent) {
          setCorsHeaders(res);
          res.writeHead(500, {
            'Content-Type': 'application/json'
          });
          res.end(JSON.stringify({ error: error.message }));
        }
      }

      return;
    }

    if (pathname === '/playlist') {
      logToFile('Processing playlist request');
      const urlParam = query.url;
      const dataParam = query.data || null;
      const epgMerging = query.epgMerging === 'true';

      if (!urlParam) {
        logToFile('No URL parameter provided');
        setCorsHeaders(res);
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('URL parameter missing');
        return;
      }

      logToFile(`Processing playlist URL: ${urlParam}`);
      await handlePlaylistRequest(req, res, urlParam, dataParam, epgMerging);
      return;
    }

    if (pathname === '/Epg') {
      const dataParam = query.data;
      if (!dataParam) {
        setCorsHeaders(res);
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        return res.end('Data parameter missing');
      }

      try {
        const mergedEpg = await epgMerger(dataParam);
        setCorsHeaders(res);
        res.writeHead(200, { 'Content-Type': 'application/xml' });
        res.end(mergedEpg);
      } catch (error) {
        logToFile('Error in epgMerger: ' + error);
        setCorsHeaders(res);
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Failed to merge EPGs');
      }
      return;
    }

    let requestUrl = query.url ? decodeURIComponent(query.url) : null;
    let secondaryUrl = query.url2 ? decodeURIComponent(query.url2) : null;
    let data = query.data ? Buffer.from(query.data, 'base64').toString() : null;
    const isMaster = !query.url2;
    let finalRequestUrl = isMaster ? requestUrl : secondaryUrl;

    if (finalRequestUrl) {
      if (query.key && query.key === 'true') {
        await fetchEncryptionKey(res, finalRequestUrl, data);
        return;
      }

      if (finalRequestUrl.includes('vipstreams.in')) {
        if (finalRequestUrl.includes('playlist.m3u8') && !finalRequestUrl.includes('&su=1') && !finalRequestUrl.includes('?id=')) {
          const path = finalRequestUrl.replace('https://rr.vipstreams.in/', '');
          const token = await StreamedSUgetSessionId(path);
          finalRequestUrl = finalRequestUrl.replace('playlist.m3u8', `playlist.m3u8?id=${token}`);
          requestUrl = encodeURIComponent(finalRequestUrl);
          const proxyUrl = `https://${req.headers.host}`;
          setCorsHeaders(res);
          res.writeHead(302, { Location: `${proxyUrl}?url=${requestUrl}&data=${encodeURIComponent(data)}&su=1&suToken=${token}&type=/index.m3u8` });
          res.end();
          return;
        } else if (query.su === '1' && query.suToken) {
          StreamedSUtokenCheck(query.suToken).catch(err => logToFile('Error in StreamedSUtokenCheck:' + err));
        }
      }

      const dataType = isMaster ? 'text' : 'binary';
      const result = await fetchContent(finalRequestUrl, data, dataType);

      if (result.status >= 400) {
        setCorsHeaders(res);
        res.writeHead(result.status, { 'Content-Type': 'text/plain' });
        res.end(`Error: ${result.status}`);
        return;
      }

      let content = result.content;

      if (isMaster) {
        const baseUrl = new URL(result.finalUrl).origin;
        const proxyUrl = `https://${req.headers.host}`;
        content = rewriteUrls(content, finalRequestUrl, proxyUrl, query.data);
      }

      setCorsHeaders(res);
      res.writeHead(result.status, {
        'Content-Type': 'application/vnd.apple.mpegurl',
        'Content-Length': Buffer.byteLength(content)
      });
      res.end(content);
      return;
    }

    setCorsHeaders(res);
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('Bad Request');

  } catch (err) {
    logToFile('Error handling request:' + err);
    if (!res.headersSent) {
      setCorsHeaders(res);
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Internal Server Error');
    }
  }
}).listen(6760, '0.0.0.0', () => {
  logToFile('Server is running on port 6760');
});

async function fetchContent(url, data, dataType = 'text') {
  logToFile(`fetchContent called with URL: ${url}`);
  try {
    if (!url) {
      throw new Error('URL is required');
    }

    const headers = {};
    if (data) {
      const headersArray = data.split('|');
      headersArray.forEach(header => {
        const [key, value] = header.split('=').map(part => part.trim());
        if (key && value) {
          headers[key] = value.replace(/['"]/g, '');
        }
      });
    }

    // Add important headers if not present
    headers['User-Agent'] = headers['User-Agent'] || 'Mozilla/5.0 (X11; Linux x86_64)';
    headers['Accept'] = dataType === 'binary' ? '*/*' : 'text/plain;charset=UTF-8';
    headers['Accept-Encoding'] = 'gzip, deflate';
    
    // For key requests, ensure we have proper headers
    if (url.includes('keylocking.ru')) {
      headers['Accept'] = '*/*';
      headers['Connection'] = 'keep-alive';
      if (!headers['Referer']) {
        // Add default referer if missing
        headers['Referer'] = new URL(url).origin;
      }
    }

    logToFile('Request headers: ' + JSON.stringify(headers));

    const response = await fetchUrl(url, headers);

    if (!response || !response.content) {
      throw new Error('No content received from URL');
    }

    let content;
    if (response.headers['content-encoding'] === 'gzip') {
      content = zlib.gunzipSync(response.content);
    } else if (response.headers['content-encoding'] === 'deflate') {
      content = zlib.inflateSync(response.content);
    } else {
      content = response.content;
    }

    // For key requests, ensure we got valid data
    if (url.includes('keylocking.ru') && (!content || content.length < 16)) {
      throw new Error('Invalid key data received');
    }

    if (dataType === 'binary') {
      logToFile(`Binary content fetched. Length: ${content.length}`);
      return {
        content,
        finalUrl: response.finalUrl || url,
        status: response.status,
        headers: response.headers,
      };
    }

    const textContent = content.toString('utf-8');
    logToFile(`Text content fetched. Length: ${textContent.length}`);
    return {
      content: textContent,
      finalUrl: response.finalUrl || url,
      status: response.status,
      headers: response.headers,
    };

  } catch (err) {
    logToFile(`Error fetching content from ${url}: ${err}`);
    return {
      content: null,
      status: err.status || 500,
      headers: {},
      error: err.message
    };
  }
}

async function handlePlaylistRequest(req, res, playlistUrl, data, epgMergingEnabled) {
  try {
    const urls = playlistUrl.split(',');
    let combinedContent = '';
    const epgUrls = new Set();
    const baseUrl = new URL(req.url, `https://${req.headers.host}`).origin;

    const excludeParam = new URL(req.url, `https://${req.headers.host}`).searchParams.get('exclude');
    const excludeGroups = excludeParam ? excludeParam.split(',').map(decodeURIComponent) : [];

    for (const singleUrl of urls) {
      const trimmedUrl = singleUrl.trim();
      logToFile("Fetching playlist URL: " + trimmedUrl);
      const result = await fetchContent(trimmedUrl, null, 'text');

      if (result.status !== 200) {
        logToFile(`Failed to fetch: ${trimmedUrl}, status: ${result.status}`);
        continue;
      }

      let playlistContent = result.content || '';
      logToFile(`Fetched playlist content length: ${playlistContent.length}`);

      const lines = playlistContent.split('\n');
      logToFile(`Number of lines in playlist: ${lines.length}`);

      const epgMatch = playlistContent.match(/#EXTM3U.*?url-tvg="(.*?)"/);
      if (epgMatch && epgMatch[1]) {
        epgUrls.add(epgMatch[1]);
        playlistContent = playlistContent.replace(epgMatch[0], '');
      } else {
        playlistContent = playlistContent.replace(/^#EXTM3U\s*\n?/, '');
      }

      if (excludeGroups.length > 0) {
        const rawLines = playlistContent.split('\n');
        let filteredContent = '';
        let skip = false;
        for (let i = 0; i < rawLines.length; i++) {
          const line = rawLines[i];
          if (line.startsWith('#EXTINF')) {
            const groupTitleMatch = line.match(/group-title="(.*?)"/);
            if (groupTitleMatch && excludeGroups.includes(groupTitleMatch[1])) {
              skip = true;
            } else {
              skip = false;
            }
          }

          if (!skip) filteredContent += line + '\n';
        }
        playlistContent = filteredContent.trim();
      }

      logToFile('Rewriting playlist URLs...');
      playlistContent = rewritePlaylistUrls(playlistContent, baseUrl, data);

      const rewrittenLines = playlistContent.split('\n').length;
      logToFile(`Number of lines after rewrite: ${rewrittenLines}`);

      combinedContent += playlistContent + '\n';
    }

    if (epgUrls.size > 1 && epgMergingEnabled) {
      const epgString = Array.from(epgUrls).join(',');
      const encodedEpg = Buffer.from(epgString).toString('base64');
      const rewrittenEpgUrl = `${baseUrl}/Epg?data=${encodedEpg}`;
      combinedContent = `#EXTM3U url-tvg="${rewrittenEpgUrl}"
${combinedContent.trim()}`;
    } else if (epgUrls.size === 1) {
      const singleEpgUrl = Array.from(epgUrls)[0];
      combinedContent = `#EXTM3U url-tvg="${singleEpgUrl}"
${combinedContent.trim()}`;
    } else if (epgUrls.size > 1 && !epgMergingEnabled) {
      combinedContent = `#EXTM3U
${combinedContent.trim()}`;
    } else {
      combinedContent = `#EXTM3U
${combinedContent.trim()}`;
    }

    logToFile(`Final combined content length: ${combinedContent.length}`);
    setCorsHeaders(res);
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end(combinedContent.trim());
  } catch (err) {
    logToFile('Error in handlePlaylistRequest:' + err);
    setCorsHeaders(res);
    res.writeHead(500, { 'Content-Type': 'text/plain' });
    res.end('Error processing playlist');
  }
}

async function fetchEncryptionKey(res, url, data) {
  try {
    // Properly decode the URL and data parameters
    const decodedUrl = decodeURIComponent(url);
    
    // Parse encoded header data
    const headerData = data ? Buffer.from(data, 'base64').toString() : '';
    const headers = {
      'Host': new URL(decodedUrl).host,
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0',
      'Accept': '*/*',
      'Accept-Language': 'en-US,en;q=0.5',
      'Accept-Encoding': 'gzip, deflate, br',
      'Origin': 'https://cookiedwebplay.xyz',
      'Referer': 'https://cookidwebplay.xyz/',
      'Sec-Fetch-Dest': 'empty',
      'Sec-Fetch-Mode': 'cors',
      'Sec-Fetch-Site': 'cross-site',
      'Te': 'trailers',
      'Connection': 'keep-alive'
    };

    // Add any additional headers from the data parameter
    if (headerData) {
      headerData.split('|').forEach(header => {
        const [key, value] = header.split('=').map(x => x.trim());
        if (key && value) {
          headers[key] = value.replace(/^["']|["']$/g, '');
        }
      });
    }

    logToFile(`Fetching key from ${decodedUrl} with headers: ${JSON.stringify(headers)}`);

    const response = await fetchUrl(decodedUrl, headers);
    
    if (response.status >= 400) {
      logToFile(`Key fetch failed: ${response.status}`);
      throw new Error(`Failed to fetch key: ${response.status}`);
    }

    // Return key with correct content type
    setCorsHeaders(res);
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'Content-Length': response.content.length
    });
    
    return res.end(response.content);

  } catch (err) {
    logToFile(`Key fetch error: ${err.message}`);
    setCorsHeaders(res);
    res.writeHead(500, {
      'Content-Type': 'text/plain'
    });
    return res.end(`Error fetching key: ${err.message}`);
  }
}

function setCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Max-Age', '86400');
}

function rewriteUrls(content, requestUrl, proxyUrl, data) {
  try {
    logToFile(`Request URL: ${requestUrl}`);

    const urlObj = new URL(requestUrl);
    const baseUrl = `${urlObj.origin}${urlObj.pathname.replace(/\/[^/]*$/, '/')}`;
    logToFile(`Base URL: ${baseUrl}`);

    const lines = content.split('\n');
    const rewrittenLines = [];
    let isNextLineMasterPlaylist = false;

    lines.forEach((line, index) => {
      line = line.trim();

      if (line.startsWith('#')) {
        if (line.includes('URI="')) {
          const uriMatch = line.match(/URI="([^"]+)"/i);
          if (uriMatch && uriMatch[1]) {
            let uri = uriMatch[1];

            if (!uri.startsWith('http')) {
              uri = new URL(uri, baseUrl).href;
              if (uri.startsWith('http:')) {
                uri = uri.replace('http:', 'https:');
              }
            }

            const rewrittenUri = `${proxyUrl}?url=${encodeURIComponent(uri)}&data=${encodeURIComponent(data)}${line.includes('#EXT-X-KEY') ? '&key=true' : ''}`;
            line = line.replace(uriMatch[1], rewrittenUri);
          }
        }

        rewrittenLines.push(line);

        if (line.includes('#EXT-X-STREAM-INF')) {
          isNextLineMasterPlaylist = true;
        } else {
          isNextLineMasterPlaylist = false;
        }
      } else if (line.trim() && !line.startsWith('#')) {
        const isMasterPlaylist = isNextLineMasterPlaylist || line.includes('.m3u8');
        const isSegment = !isMasterPlaylist;

        const urlParam = isSegment ? 'url2' : 'url';
        let lineUrl = line;

        if (!lineUrl.startsWith('http')) {
          lineUrl = new URL(lineUrl, baseUrl).href;
          if (lineUrl.startsWith('http:')) {
            lineUrl = lineUrl.replace('http:', 'https:');
          }
        }

        const fullUrl = `${proxyUrl}?${urlParam}=${encodeURIComponent(lineUrl)}&data=${encodeURIComponent(data)}${isSegment ? '&type=/index.ts' : '&type=/index.m3u8'}`;
        rewrittenLines.push(fullUrl);

        isNextLineMasterPlaylist = false;
      } else {
        rewrittenLines.push(line);
      }
    });

    return rewrittenLines.join('\n');
  } catch (err) {
    logToFile('Error in rewriteUrls:' + err);
    return content;
  }
}

function fetchUrl(requestUrl, headers, redirectCount = 0) {
  return new Promise((resolve, reject) => {
    try {
      if (redirectCount > 10) {
        return reject(new Error('Too many redirects'));
      }

      let parsedUrl;
      try {
        parsedUrl = new URL(requestUrl);
      } catch (e) {
        return reject(new Error('Invalid URL: ' + requestUrl));
      }

      const isHttps = parsedUrl.protocol === 'https:';
      const options = {
        hostname: parsedUrl.hostname,
        port: parsedUrl.port || (isHttps ? 443 : 80),
        path: parsedUrl.pathname + (parsedUrl.search || ''),
        method: 'GET',
        headers: {
          ...headers,
          'Accept': '*/*',
          'Accept-Encoding': 'gzip, deflate',
          'Connection': 'keep-alive'
        },
        timeout: 60000,
        followRedirect: true,
      };

      const httpModule = isHttps ? https : http;

      const req = httpModule.request(options, res => {
        const statusCode = res.statusCode;

        if (statusCode >= 300 && statusCode < 400 && res.headers.location) {
          logToFile(`Following redirect to: ${res.headers.location}`);
          const redirectUrl = new URL(res.headers.location, requestUrl).href;
          return fetchUrl(redirectUrl, headers, redirectCount + 1)
            .then(resolve)
            .catch(reject);
        }

        res.setTimeout(60000);
        const chunks = [];

        res.on('data', chunk => chunks.push(chunk));

        res.on('end', () => {
          const content = Buffer.concat(chunks);
          resolve({
            content,
            finalUrl: requestUrl,
            status: statusCode,
            headers: res.headers,
          });
        });
      });

      req.on('error', (error) => {
        logToFile(`Request error for ${requestUrl}: ${error}`);
        reject(new Error(`Request failed: ${error.message}`));
      });

      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Request timed out'));
      });

      req.end();

    } catch (err) {
      logToFile(`Error in fetchUrl for ${requestUrl}: ${err}`);
      reject(err);
    }
  });
}

function fetchEpgContent(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;

    client.get(url, (response) => {
      const statusCode = response.statusCode;
      const headers = response.headers || {};

      if (statusCode !== 200) {
        response.resume();
        return reject(new Error(`Request failed with status code: ${statusCode}`));
      }

      const chunks = [];
      response.on('data', (chunk) => chunks.push(chunk));
      response.on('end', () => {
        const buffer = Buffer.concat(chunks);
        const encoding = headers['content-encoding'] || '';

        try {
          let content;
          if (encoding.includes('gzip')) content = zlib.gunzipSync(buffer);
          else if (encoding.includes('deflate')) content = zlib.inflateSync(buffer);
          else content = buffer;

          resolve(content.toString('utf-8'));
        } catch (decompressionError) {
          reject(new Error(`Decompression failed: ${decompressionError.message}`));
        }
      });
    }).on('error', (error) => reject(error));
  });
}

function rewritePlaylistUrls(content, baseUrl, data) {
  try {
    logToFile('rewritePlaylistUrls called. Content length: ' + content.length);
    const lines = content.split('\n');
    const rewrittenLines = [];
    let vlcHeaders = [];
    let channelCount = 0;

    lines.forEach(line => {
      if (line.startsWith('#EXTINF')) {
        channelCount++;
        rewrittenLines.push(line);
      } else if (line.startsWith('http') && !line.includes('inputstream.adaptive')) {
        const headerSeparatorIndex = line.indexOf('|');
        const streamUrl = headerSeparatorIndex !== -1 ? line.substring(0, headerSeparatorIndex) : line;

        let base64Data = '';
        if (headerSeparatorIndex !== -1) {
          const headersString = line.substring(headerSeparatorIndex + 1);
          const decodedHeadersString = decodeURIComponent(headersString);
          const headers = decodedHeadersString
            ? decodedHeadersString.split('&').map(header => {
              const [key, ...valueParts] = header.split('=');
              let cleanKey = key.trim();
              const cleanValue = valueParts.join('=').replace(/^['"]|['"]$/g, '').trim();
              if (cleanKey === 'referrer') cleanKey = 'Referer';
              return `${cleanKey}=${cleanValue}`;
            })
            : [];

          base64Data = headers.length > 0 ? Buffer.from(headers.join('|')).toString('base64') : '';
        } else if (vlcHeaders.length > 0) {
          const formattedVlcHeaders = vlcHeaders.map(header => {
            const [key, value] = header.split('=');
            let cleanKey = key.replace('http-', '').trim();
            if (cleanKey === 'referrer') cleanKey = 'Referer';
            const capitalizedKey = cleanKey.charAt(0).toUpperCase() + cleanKey.slice(1);
            const cleanValue = value ? value.replace(/^['"]|['"]$/g, '').trim() : '';
            return `${capitalizedKey}=${cleanValue}`;
          });
          base64Data = Buffer.from(formattedVlcHeaders.join('|')).toString('base64');
          vlcHeaders = [];
        } else if (data) {
          base64Data = data;
        }

        const newUrl = base64Data
          ? `${baseUrl}?url=${encodeURIComponent(streamUrl)}&data=${encodeURIComponent(base64Data)}`
          : line;

        rewrittenLines.push(newUrl);
      } else if (line.startsWith('#EXTVLCOPT:http-')) {
        const headerSeparatorIndex = line.indexOf(':');
        if (headerSeparatorIndex !== -1) {
          const header = line.substring(headerSeparatorIndex + 1).trim().replace(/^['"]|['"]$/g, '');
          vlcHeaders.push(header);
        }
      } else if (line.includes('inputstream.adaptive')) {
        rewrittenLines.push(line);
      } else if (!line.startsWith('#EXTVLCOPT') && !line.startsWith('#KODIPOP')) {
        rewrittenLines.push(line);
      }
    });

    logToFile(`rewritePlaylistUrls processed ${channelCount} channel entries.`);
    return rewrittenLines.join('\n');
  } catch (err) {
    logToFile('Error in rewritePlaylistUrls:' + err);
    return content;
  }
}

async function epgMerger(encodedData) {
  const urls = Buffer.from(encodedData, 'base64').toString('utf-8').split(',');
  let mergedEpg = '';

  for (const url of urls) {
    try {
      const epgContent = await fetchEpgContent(url.trim());
      mergedEpg += epgContent.replace(/<\?xml.*?\?>/, '').replace(/<\/?tv>/g, '');
    } catch (error) {
      logToFile('Failed to fetch or parse EPG:' + error);
    }
  }

  return `<?xml version="1.0" encoding="UTF-8"?><tv>${mergedEpg}</tv>`;
}

async function StreamedSUgetSessionId(path) {
  const sessionKey = getSessionKey(path);
  const currentTime = Date.now();
  const sessionData = await getSessionToken(sessionKey);

  if (sessionData) {
    const lastChecked = await getLastCheckedTimestamp(sessionData.token);

    if (currentTime - sessionData.timestamp < 2 * 60 * 60 * 1000 && lastChecked && currentTime - lastChecked < 30000) {
      logToFile('Using cached Streamed Su Token:' + sessionData.token);
      return sessionData.token;
    } else {
      logToFile('Token expired or not checked recently enough. Creating new token...');
    }
  }

  const targetUrl = "https://secure.bigcoolersonline.top/init-session";
  const sendPath = '/' + path;
  logToFile('Fetching new Streamed Su Token for path:' + sendPath);

  try {
    const postData = JSON.stringify({ path: sendPath });

    const options = {
      hostname: "secure.bigcoolersonline.top",
      path: "/init-session",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
        "Referer": "https://embedme.top/",
      },
    };

    const token = await new Promise((resolve, reject) => {
      const req = https.request(options, (res) => {
        let data = "";

        res.on("data", (chunk) => {
          data += chunk;
        });

        res.on("end", () => {
          if (res.statusCode === 200) {
            const responseData = JSON.parse(data);
            logToFile('Fetched new Streamed Su Token:' + responseData.id);
            resolve(responseData.id);
          } else {
            reject(new Error(`Failed to fetch session data: ${res.statusCode}`));
          }
        });
      });

      req.on("error", (error) => reject(error));
      req.write(postData);
      req.end();
    });

    await setSessionToken(sessionKey, token, currentTime);
    return token;
  } catch (error) {
    logToFile("Error:" + error);
    throw error;
  }
}

async function StreamedSUtokenCheck(token) {
  const currentTime = Date.now();
  const lastChecked = await getLastCheckedTimestamp(token);
  if (lastChecked && currentTime - lastChecked < 15000) {
    logToFile(`Skipping StreamedSUtokenCheck for ${token} due to timestamp.`);
    return null;
  }

  const checkUrl = `https://secure.bigcoolersonline.top/check/${token}`;
  logToFile('Checking Streamed Su Token: ' + token);

  const options = {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
      "Referer": "https://embedme.top/",
    },
    timeout: 7000,
  };

  return new Promise((resolve, reject) => {
    const req = https.request(checkUrl, options, (res) => {
      let data = "";

      res.on("data", (chunk) => {
        data += chunk;
      });

      res.on("end", async () => {
        if (res.statusCode === 200) {
          await setLastCheckedTimestamp(token, currentTime);
          resolve(data);
        } else if (res.statusCode === 429) {
          logToFile("Rate limit exceeded: 429 error.");
          resolve(null);
		} else if (res.statusCode === 400) {
		  logToFile("Bad token! Attempting to force a new token.");
		  await setLastCheckedTimestamp(token, currentTime - 30000);
          resolve(null);
        } else {
          reject(new Error(`Failed to check token: ${res.statusCode}`));
        }
      });
    });

    req.on("error", (error) => reject(error));
    req.on("timeout", () => {
      req.destroy();
      reject(new Error("Request timed out"));
    });

    req.end();
  });
}

function initializeStorage() {
  if (process.env.VERCEL) return require('@vercel/kv');
  if (process.env.REDIS_URL) return new Redis(process.env.REDIS_URL);
  return null;
}

function getSessionKey(path) {
  const parts = path.split('/');
  return parts.slice(0, 5).join('/');
}

async function getSessionToken(path) {
  const key = `sessionToken:${path}`;
  if (storage && storage.get) {
    const tokenData = await storage.get(key);
    return tokenData ? JSON.parse(tokenData) : null;
  }
  return sessionTokens[key];
}

async function setSessionToken(path, token, timestamp) {
  const key = `sessionToken:${path}`;
  const tokenData = JSON.stringify({ token, timestamp });
  if (storage && storage.set) {
    await storage.set(key, tokenData);
  } else {
    sessionTokens[key] = { token, timestamp };
  }
}

async function getLastCheckedTimestamp(token) {
  if (storage && storage.get) return await storage.get(`lastCheckedTimestamp:${token}`);
  return lastCheckedTimestamps[token];
}

async function setLastCheckedTimestamp(token, timestamp) {
  if (storage && storage.set) await storage.set(`lastCheckedTimestamp:${token}`, timestamp);
  else lastCheckedTimestamps[token] = timestamp;
}

function setCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, HEAD');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length');
}
