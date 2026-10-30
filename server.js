'use strict';

// Vault Run: one-file Node.js + ws server. Install ws with: npm install ws
// Run with: node server.js (defaults to http://localhost:3000)
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const IDLE_TTL_MS = 30 * 60 * 1000;
const ROUND_SECONDS = 45;
const MAX_MESSAGE_BYTES = 8 * 1024;
const rooms = new Map();
const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ';

const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, 'http://' + (req.headers.host || 'localhost')).pathname;
  if (req.method !== 'GET' || (pathname !== '/' && pathname !== '/index.html')) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end('Not found');
  }
  fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
    if (err) {
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
      return res.end('Could not load index.html');
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(data);
  });
});
const wss = new WebSocketServer({ server, maxPayload: MAX_MESSAGE_BYTES });

function tokenHash(token) { return crypto.createHash('sha256').update(token).digest('hex'); }
function send(ws, event, data) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ event, ...(data || {}) }));
}
function sendPlayer(player, event, data) { send(player.socket, event, data); }
function touch(room) { room.lastActivityAt = Date.now(); }
function connected(player) { return !!player.socket && player.socket.readyState === WebSocket.OPEN; }
function roomFor(ws) { return ws.roomCode ? rooms.get(ws.roomCode) : null; }
function playerFor(ws, room) { return room && room.players.find((p) => p.id === ws.playerId) || null; }
function broadcast(room, event, data) {
  room.players.forEach((p) => sendPlayer(p, event, data));
}
function lobbyPayload(room) {
  return {
    code: room.code,
    hostPlayerId: room.hostPlayerId,
    status: room.game.status,
    players: room.players.map((p) => ({ id: p.id, name: p.name, connected: connected(p), score: p.score })),
    canStart: room.players.filter(connected).length >= 3 && room.players.length <= 6
  };
}
function updateLobby(room) { broadcast(room, 'lobby_update', lobbyPayload(room)); }
function roomCode() {
  let code;
  do {
    code = Array.from({ length: 4 }, () => alphabet[crypto.randomInt(alphabet.length)]).join('');
  } while (rooms.has(code));
  return code;
}
function cleanName(value) {
  const name = String(value || '').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, 20);
  return name || 'Player';
}
function attach(ws, room, player) {
  if (player.socket && player.socket !== ws) {
    player.socket.roomCode = null;
    player.socket.playerId = null;
    try { player.socket.close(4001, 'Seat reconnected elsewhere'); } catch (_) {}
  }
  player.socket = ws;
  ws.roomCode = room.code;
  ws.playerId = player.id;
  touch(room);
}
function connectionState(player) {
  const room = rooms.get(player.socket && player.socket.roomCode);
  if (!room) return;
  sendPlayer(player, 'joined_room', { code: room.code, playerId: player.id, name: player.name });
  sendPlayer(player, 'lobby_update', lobbyPayload(room));
  if (room.game.status === 'lobby') {
    return;
  }
  if (room.game.status === 'playing') {
    sendRoundStart(room, player);
    sendPlayer(player, 'choice_locked', {
      locked: player.choice !== null,
      lockedCount: room.players.filter((p) => p.choice !== null).length,
      total: room.players.length
    });
    maybeSendHint(room);
    if (player.id === room.game.insiderPlayerId && room.game.hintSent && room.game.lastHint) sendPlayer(player, 'insider_hint', room.game.lastHint);
    return;
  }
  if (room.game.status === 'between_rounds') {
    if (room.game.lastReveal) sendPlayer(player, 'round_revealed', room.game.lastReveal);
    sendPlayer(player, 'round_intermission', {
      round: room.game.round,
      nextRound: room.game.round + 1,
      nextVault: 6 + room.game.round * 2 + room.game.carryover,
      startsAt: room.game.nextRoundAt,
      score: player.score
    });
    return;
  }
  if (room.game.lastReveal) sendPlayer(player, 'round_revealed', room.game.lastReveal);
  if (room.game.status === 'finished') {
    sendPlayer(player, 'game_finished', { scoreboard: scoreboard(room), winnerIds: winnerIds(room) });
  }
}
function scoreboard(room) {
  return room.players.map((p) => ({ id: p.id, name: p.name, score: p.score }))
    .sort((a, b) => b.score - a.score || a.name.localeCompare(b.name));
}
function winnerIds(room) {
  const high = Math.max(...room.players.map((p) => p.score));
  return room.players.filter((p) => p.score === high).map((p) => p.id);
}
function sendRoundStart(room, onlyPlayer) {
  const g = room.game;
  const payloadFor = (p) => ({
    round: g.round,
    totalRounds: 5,
    vault: g.currentVault,
    deadline: g.deadline,
    durationSeconds: ROUND_SECONDS,
    locked: p.choice !== null,
    score: p.score,
    insider: p.id === g.insiderPlayerId
  });
  (onlyPlayer ? [onlyPlayer] : room.players).forEach((p) => sendPlayer(p, 'round_started', payloadFor(p)));
}
function maybeSendHint(room) {
  const g = room.game;
  if (g.status !== 'playing' || !g.insiderPlayerId || g.hintSent) return;
  const insider = room.players.find((p) => p.id === g.insiderPlayerId);
  const target = room.players.find((p) => p.id === g.insiderTargetPlayerId);
  if (!insider || !target || !connected(insider)) return;
  if (target.choice !== null && insider.choice === null) {
    g.hintSent = true;
    g.lastHint = { state: 'ready', targetName: target.name, choice: target.choice };
    sendPlayer(insider, 'insider_hint', g.lastHint);
  } else if (insider.choice !== null) {
    g.hintSent = true;
    g.lastHint = { state: 'too_late', message: 'Your informant was too slow this round.' };
    sendPlayer(insider, 'insider_hint', g.lastHint);
  }
}
function startRound(room, round) {
  const g = room.game;
  if (room.timer) clearTimeout(room.timer);
  if (g.nextRoundTimer) clearTimeout(g.nextRoundTimer);
  room.timer = null;
  g.nextRoundTimer = null;
  g.nextRoundAt = null;
  g.status = 'playing';
  g.round = round;
  g.currentVault = 6 + (round - 1) * 2 + g.carryover;
  g.deadline = Date.now() + ROUND_SECONDS * 1000;
  g.insiderPlayerId = room.players[crypto.randomInt(room.players.length)].id;
  const targets = room.players.filter((p) => p.id !== g.insiderPlayerId);
  g.insiderTargetPlayerId = targets[crypto.randomInt(targets.length)].id;
  g.hintSent = false;
  g.lastHint = null;
  g.lastReveal = null;
  room.players.forEach((p) => { p.choice = null; p.autoReason = null; });
  sendRoundStart(room);
  room.timer = setTimeout(() => timeoutRound(room.code, round), ROUND_SECONDS * 1000);
  room.timer.unref && room.timer.unref();
}
function startGame(room) {
  if (room.timer) clearTimeout(room.timer);
  room.players.forEach((p) => { p.score = 0; p.choice = null; p.autoReason = null; });
  room.game = {
    status: 'playing', round: 0, carryover: 0, currentVault: 0, deadline: 0,
    insiderPlayerId: null, insiderTargetPlayerId: null, hintSent: false, lastHint: null,
    lastReveal: null, finalTimer: null
  };
  touch(room);
  broadcast(room, 'game_started', { totalRounds: 5 });
  startRound(room, 1);
}
function evaluateRound(room, reason) {
  const g = room.game;
  if (g.status !== 'playing') return;
  if (reason === 'timeout') {
    room.players.forEach((p) => {
      if (p.choice === null) { p.choice = 'SHARE'; p.autoReason = connected(p) ? 'timeout' : 'disconnected'; }
    });
  } else {
    room.players.forEach((p) => {
      if (!connected(p) && p.choice === null) { p.choice = 'SHARE'; p.autoReason = 'disconnected'; }
    });
  }
  if (room.players.some((p) => connected(p) && p.choice === null)) return;
  if (room.timer) clearTimeout(room.timer);
  room.timer = null;

  const grabs = room.players.filter((p) => p.choice === 'GRAB');
  let perPlayer = 0;
  let remainder = 0;
  let outcome;
  if (grabs.length === room.players.length) {
    outcome = 'alarm';
    remainder = g.currentVault;
  } else if (grabs.length > 0) {
    outcome = 'grab';
    perPlayer = Math.floor(g.currentVault / grabs.length);
    remainder = g.currentVault % grabs.length;
    grabs.forEach((p) => { p.score += perPlayer; });
  } else {
    outcome = 'share';
    perPlayer = Math.floor(g.currentVault / room.players.length);
    remainder = g.currentVault % room.players.length;
    room.players.forEach((p) => { p.score += perPlayer; });
  }
  g.carryover = g.round < 5 ? remainder : 0;
  const reveal = {
    round: g.round,
    vault: g.currentVault,
    outcome,
    coinsPerWinner: perPlayer,
    coinsLeft: remainder,
    carryover: g.round < 5 ? remainder : 0,
    players: room.players.map((p) => ({
      id: p.id, name: p.name, choice: p.choice, score: p.score,
      gain: outcome === 'alarm' ? 0 : (outcome === 'share' ? perPlayer : (p.choice === 'GRAB' ? perPlayer : 0)),
      autoReason: p.autoReason,
      connected: connected(p)
    }))
  };
  g.lastReveal = reveal;
  touch(room);
  broadcast(room, 'round_revealed', reveal);
  if (g.round === 5) {
    g.status = 'finished';
    g.finalTimer = setTimeout(() => {
      if (rooms.get(room.code) !== room || room.game.status !== 'finished') return;
      broadcast(room, 'game_finished', { scoreboard: scoreboard(room), winnerIds: winnerIds(room) });
    }, 4500);
    g.finalTimer.unref && g.finalTimer.unref();
  } else {
    g.status = 'between_rounds';
    g.nextRoundAt = Date.now() + 5500;
    g.nextRoundTimer = setTimeout(() => {
      if (rooms.get(room.code) === room && room.game.status === 'between_rounds') startRound(room, g.round + 1);
    }, 5500);
    g.nextRoundTimer.unref && g.nextRoundTimer.unref();
  }
}
function timeoutRound(code, round) {
  const room = rooms.get(code);
  if (room && room.game.status === 'playing' && room.game.round === round) evaluateRound(room, 'timeout');
}
function maybeResolve(room) {
  if (room.game.status !== 'playing') return;
  if (room.players.filter(connected).every((p) => p.choice !== null)) evaluateRound(room, 'all_connected_locked');
}
function fail(ws, message) { send(ws, 'error', { message }); }
function handle(ws, msg) {
  const event = msg && msg.event;
  if (!event || typeof event !== 'string') return fail(ws, 'Invalid message.');
  if (event === 'ping') {
    const room = roomFor(ws); if (room) touch(room);
    return send(ws, 'pong', { now: Date.now() });
  }
  if (event === 'create_room') {
    if (ws.playerId) return fail(ws, 'You are already in a room.');
    const code = roomCode();
    const token = crypto.randomBytes(32).toString('hex');
    const player = { id: crypto.randomUUID(), tokenHash: tokenHash(token), name: cleanName(msg.name), score: 0, choice: null, autoReason: null, socket: ws };
    const room = {
      code, createdAt: Date.now(), lastActivityAt: Date.now(), hostPlayerId: player.id,
      players: [player], game: { status: 'lobby', round: 0, carryover: 0 }, timer: null
    };
    rooms.set(code, room); ws.roomCode = code; ws.playerId = player.id;
    send(ws, 'room_created', { code, playerId: player.id, token, name: player.name });
    updateLobby(room); return;
  }
  if (event === 'join_room') {
    if (ws.playerId) return fail(ws, 'You are already in a room.');
    const code = String(msg.code || '').toUpperCase();
    const room = rooms.get(code);
    if (!room) return fail(ws, 'Room not found. Check the 4-letter code.');
    if (room.game.status !== 'lobby') return fail(ws, 'This game has already started.');
    if (room.players.length >= 6) return fail(ws, 'This room is full (6 players).');
    const token = crypto.randomBytes(32).toString('hex');
    const player = { id: crypto.randomUUID(), tokenHash: tokenHash(token), name: cleanName(msg.name), score: 0, choice: null, autoReason: null, socket: ws };
    room.players.push(player); ws.roomCode = code; ws.playerId = player.id; touch(room);
    send(ws, 'joined_room', { code, playerId: player.id, token, name: player.name });
    updateLobby(room); return;
  }
  if (event === 'reconnect') {
    if (ws.playerId) return fail(ws, 'This connection is already in a room.');
    const code = String(msg.code || '').toUpperCase();
    const room = rooms.get(code);
    const hash = tokenHash(String(msg.token || ''));
    const player = room && room.players.find((p) => p.tokenHash === hash);
    if (!player) return fail(ws, 'Saved seat not found. Create or join a room.');
    attach(ws, room, player); connectionState(player); return;
  }
  const room = roomFor(ws);
  const player = playerFor(ws, room);
  if (!room || !player) return fail(ws, 'Join a room first.');
  touch(room);
  if (event === 'start_game') {
    if (room.game.status !== 'lobby') return fail(ws, 'The game is not in the lobby.');
    if (player.id !== room.hostPlayerId) return fail(ws, 'Only the host can start the game.');
    if (room.players.filter(connected).length < 3) return fail(ws, 'At least 3 connected players are needed.');
    startGame(room); return;
  }
  if (event === 'lock_choice') {
    if (room.game.status !== 'playing') return fail(ws, 'The round is not accepting choices.');
    if (msg.choice !== 'SHARE' && msg.choice !== 'GRAB') return fail(ws, 'Choose SHARE or GRAB.');
    if (player.choice !== null) return fail(ws, 'Your choice is already locked.');
    player.choice = msg.choice;
    sendPlayer(player, 'choice_locked', { locked: true, lockedCount: room.players.filter((p) => p.choice !== null).length, total: room.players.length });
    maybeSendHint(room);
    room.players.forEach((p) => {
      if (p.id !== player.id) sendPlayer(p, 'choice_progress', { lockedCount: room.players.filter((x) => x.choice !== null).length, total: room.players.length });
    });
    maybeResolve(room); return;
  }
  if (event === 'play_again') {
    if (room.game.status !== 'finished') return fail(ws, 'The game has not finished yet.');
    if (player.id !== room.hostPlayerId) return fail(ws, 'Only the host can start another game.');
    if (room.game.finalTimer) clearTimeout(room.game.finalTimer);
    room.game = { status: 'lobby', round: 0, carryover: 0 };
    room.players.forEach((p) => { p.score = 0; p.choice = null; p.autoReason = null; });
    touch(room); broadcast(room, 'game_reset', { code: room.code }); updateLobby(room); return;
  }
  if (event === 'leave_room') {
    if (room.game.status !== 'lobby') return fail(ws, 'Seats stay reserved until the game ends.');
    room.players = room.players.filter((p) => p.id !== player.id);
    ws.roomCode = null; ws.playerId = null; player.socket = null;
    if (room.players.length === 0) rooms.delete(room.code);
    else {
      if (room.hostPlayerId === player.id) room.hostPlayerId = room.players[0].id;
      updateLobby(room);
    }
    send(ws, 'left_room', {}); return;
  }
  fail(ws, 'Unknown event.');
}

wss.on('connection', (ws) => {
  ws.roomCode = null; ws.playerId = null;
  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (_) { return fail(ws, 'Message must be JSON.'); }
    try { handle(ws, msg); } catch (err) { console.error('Message handling error:', err); fail(ws, 'Something went wrong. Please try again.'); }
  });
  ws.on('close', () => {
    const room = roomFor(ws);
    const player = playerFor(ws, room);
    if (!room || !player || player.socket !== ws) return;
    player.socket = null; touch(room);
    if (room.game.status === 'lobby') {
      if (room.hostPlayerId === player.id) {
        const nextHost = room.players.find((p) => connected(p));
        if (nextHost) room.hostPlayerId = nextHost.id;
      }
      updateLobby(room);
    } else if (room.game.status === 'playing') maybeResolve(room);
  });
});

setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.lastActivityAt >= IDLE_TTL_MS) {
      if (room.timer) clearTimeout(room.timer);
      if (room.game.nextRoundTimer) clearTimeout(room.game.nextRoundTimer);
      if (room.game.finalTimer) clearTimeout(room.game.finalTimer);
      room.players.forEach((p) => { if (connected(p)) { sendPlayer(p, 'room_expired', {}); p.socket.close(1000, 'Room expired'); } });
      rooms.delete(code);
    }
  }
}, 60 * 1000).unref();

server.listen(PORT, () => console.log('Vault Run listening on http://localhost:' + PORT));
