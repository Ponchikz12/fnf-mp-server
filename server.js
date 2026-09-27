// FNF-style multiplayer lobby relay server.
//
// Responsibilities:
//   - create/join lobbies with codes like "FNFLOBBY-XXXXXX"
//   - public lobby listing
//   - relay the host's chosen song (JSON only — no audio bytes) to the guest
//   - relay each player's chosen character (one per player, as a data-URL atlas — never the
//     sender's whole local library) so both sides can render each other correctly
//   - relay ready state and a synced countdown start (same absolute timestamp to both clients)
//   - relay live hit/miss/finish events between the two players during a match
//
// This server does NOT transmit audio files. Both players must have the same
// Inst.ogg / Voices.ogg loaded locally (matched by filename+duration on the client).
//
// Run:
//   npm install
//   npm start
// Then point the game's "Multiplayer" screen at ws://<host>:<port> (or wss:// once
// you put it behind TLS, which you should for a public deployment / Telegram Mini App).

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
// no 0/O/1/I/L — avoids visually-confusable characters in lobby codes
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
// a character carries one atlas (+ optional icon) as a base64 data URL — generous but bounded,
// so one oversized sprite sheet can't be used to flood the relay
const MAX_CHAR_JSON_BYTES = 8 * 1024 * 1024;

function genCode() {
  let s = '';
  for (let i = 0; i < 6; i++) s += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
  return 'FNFLOBBY-' + s;
}

/** @type {Map<string, {code:string, isPrivate:boolean, players:Map<import('ws').WebSocket,{id:number,name:string,ready:boolean,char:any}>, song:any, hostWs:import('ws').WebSocket}>} */
const lobbies = new Map();
let nextPlayerId = 1;

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
function broadcast(lobby, obj, exceptWs) {
  for (const ws of lobby.players.keys()) {
    if (ws !== exceptWs) send(ws, obj);
  }
}
function lobbyPlayerList(lobby) {
  return [...lobby.players.values()].map(p => ({ id: p.id, name: p.name, ready: p.ready, device: p.device }));
}
function publicLobbyList() {
  const out = [];
  for (const lobby of lobbies.values()) {
    if (!lobby.isPrivate && lobby.players.size < 2) {
      const host = [...lobby.players.values()][0];
      out.push({ code: lobby.code, hostName: host ? host.name : '?', playerCount: lobby.players.size });
    }
  }
  return out;
}
function leaveLobby(ws) {
  if (!ws.lobbyCode) return;
  const lobby = lobbies.get(ws.lobbyCode);
  if (!lobby) return;
  lobby.players.delete(ws);
  broadcast(lobby, { type: 'player_left', id: ws.playerId }, ws);
  if (lobby.players.size === 0) {
    lobbies.delete(lobby.code);
  } else if (lobby.hostWs === ws) {
    lobby.hostWs = [...lobby.players.keys()][0];
  }
  ws.lobbyCode = null;
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('FNF multiplayer relay is running. Lobbies open: ' + lobbies.size + '\n');
});
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.playerId = nextPlayerId++;
  ws.lobbyCode = null;
  ws.playerName = 'Player' + ws.playerId;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.type === 'ping') {
      // works even before joining a lobby — lets the client show its own latency to the server
      send(ws, { type: 'pong', t: msg.t });
      return;
    }

    if (msg.type === 'create_lobby') {
      let code;
      do { code = genCode(); } while (lobbies.has(code));
      const lobby = { code, isPrivate: !!msg.isPrivate, players: new Map(), song: null, hostWs: ws };
      ws.playerName = (msg.name || ws.playerName).slice(0, 24);
      ws.device = msg.device === 'mobile' ? 'mobile' : 'desktop';
      lobby.players.set(ws, { id: ws.playerId, name: ws.playerName, ready: false, device: ws.device, char: null });
      lobbies.set(code, lobby);
      ws.lobbyCode = code;
      send(ws, { type: 'lobby_created', code, isPrivate: lobby.isPrivate, players: lobbyPlayerList(lobby), youId: ws.playerId, isHost: true });
      return;
    }

    if (msg.type === 'join_lobby') {
      const code = String(msg.code || '').trim().toUpperCase();
      const lobby = lobbies.get(code);
      if (!lobby) { send(ws, { type: 'error', message: 'Лобби с таким кодом не найдено.' }); return; }
      if (lobby.players.size >= 2) { send(ws, { type: 'error', message: 'Лобби уже заполнено.' }); return; }
      ws.playerName = (msg.name || ws.playerName).slice(0, 24);
      ws.device = msg.device === 'mobile' ? 'mobile' : 'desktop';
      lobby.players.set(ws, { id: ws.playerId, name: ws.playerName, ready: false, device: ws.device, char: null });
      ws.lobbyCode = lobby.code;
      send(ws, { type: 'lobby_joined', code: lobby.code, isPrivate: lobby.isPrivate, players: lobbyPlayerList(lobby), youId: ws.playerId, isHost: false });
      broadcast(lobby, { type: 'player_joined', player: { id: ws.playerId, name: ws.playerName, ready: false, device: ws.device } }, ws);
      if (lobby.song) send(ws, { type: 'song_sync', song: lobby.song });
      // catch up the joiner on whichever character(s) the other player(s) already picked —
      // just theirs, not a library, same as a fresh set_char relay
      for (const [otherWs, p] of lobby.players) {
        if (otherWs !== ws && p.char) send(ws, { type: 'char_sync', char: p.char, from: p.id });
      }
      return;
    }

    if (msg.type === 'list_public') {
      send(ws, { type: 'public_list', lobbies: publicLobbyList() });
      return;
    }

    const lobby = ws.lobbyCode ? lobbies.get(ws.lobbyCode) : null;
    if (!lobby) return;

    if (msg.type === 'leave_lobby') { leaveLobby(ws); return; }

    if (msg.type === 'set_song') {
      if (ws !== lobby.hostWs) return; // only the host's song counts
      lobby.song = msg.song;
      broadcast(lobby, { type: 'song_sync', song: msg.song }, ws);
      return;
    }

    if (msg.type === 'set_roles') {
      if (ws !== lobby.hostWs) return; // only the host assigns roles
      lobby.roles = msg.roles;
      broadcast(lobby, { type: 'roles_sync', roles: msg.roles }, ws);
      return;
    }

    if (msg.type === 'set_char') {
      // either player sends their OWN single character — never a whole library, so there's
      // nothing to filter here beyond a basic size guard against an oversized atlas
      if (Buffer.byteLength(raw) > MAX_CHAR_JSON_BYTES) {
        send(ws, { type: 'error', message: 'Файл персонажа слишком большой для передачи сопернику.' });
        return;
      }
      const p = lobby.players.get(ws);
      if (p) p.char = msg.char || null;
      broadcast(lobby, { type: 'char_sync', char: msg.char || null, from: ws.playerId }, ws);
      return;
    }

    if (msg.type === 'ready' || msg.type === 'unready') {
      const p = lobby.players.get(ws);
      if (p) p.ready = (msg.type === 'ready');
      broadcast(lobby, { type: 'player_ready', id: ws.playerId, ready: p ? p.ready : false }, null);
      const all = [...lobby.players.values()];
      if (all.length === 2 && all.every(pl => pl.ready) && lobby.song) {
        const startAt = Date.now() + 3000; // same absolute timestamp sent to both — that's the sync
        broadcast(lobby, { type: 'game_start', startAt }, null);
      }
      return;
    }

    if (msg.type === 'hit' || msg.type === 'miss' || msg.type === 'finish' || msg.type === 'note_event' || msg.type === 'ping_report') {
      const relay = Object.assign({}, msg, { type: 'opponent_' + msg.type, from: ws.playerId });
      broadcast(lobby, relay, ws);
      return;
    }
  });

  ws.on('close', () => leaveLobby(ws));
});

server.listen(PORT, () => console.log('FNF multiplayer relay listening on :' + PORT));
