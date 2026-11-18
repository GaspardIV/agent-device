import { describe, expect, it } from 'vitest';
import {
  DAEMON_CONTROL_PROTOCOL_VERSION,
  createDaemonControlNonce,
  isValidControlNonce,
  parseControlChallengeRequest,
  parseControlChallengeResponse,
  parseControlCommandRequest,
  parseControlCommandResponse,
  signDaemonControlChallenge,
  signDaemonControlCommand,
  signDaemonControlReply,
  verifyDaemonControlChallenge,
  verifyDaemonControlCommand,
  verifyDaemonControlReply,
} from './daemon-control.ts';
import { DAEMON_QUIESCE_COMMAND, DAEMON_SHUTDOWN_COMMAND } from './daemon-resolution.ts';

describe('daemon-control contract', () => {
  const token = 'secret-daemon-token';
  const fence = { pid: 4242, processStartTime: 'Mon Nov 17 12:00:00 2026' } as const;

  it('signs and verifies challenge, command, and reply proofs with domain separation', () => {
    const clientNonce = createDaemonControlNonce();
    const serverNonce = createDaemonControlNonce();
    expect(isValidControlNonce(clientNonce)).toBe(true);
    expect(isValidControlNonce('too-short')).toBe(false);
    expect(isValidControlNonce('zz'.repeat(16))).toBe(false);

    const challengeProof = signDaemonControlChallenge({
      token,
      fence,
      clientNonce,
      serverNonce,
    });
    expect(
      verifyDaemonControlChallenge({
        token,
        fence,
        clientNonce,
        serverNonce,
        proof: challengeProof,
      }),
    ).toBe(true);
    expect(
      verifyDaemonControlChallenge({
        token: 'wrong-token',
        fence,
        clientNonce,
        serverNonce,
        proof: challengeProof,
      }),
    ).toBe(false);
    expect(
      verifyDaemonControlChallenge({
        token,
        fence: { ...fence, pid: 4243 },
        clientNonce,
        serverNonce,
        proof: challengeProof,
      }),
    ).toBe(false);

    const commandProof = signDaemonControlCommand({
      token,
      command: DAEMON_SHUTDOWN_COMMAND,
      fence,
      clientNonce,
      serverNonce,
      requestId: 'req-1',
    });
    expect(
      verifyDaemonControlCommand({
        token,
        command: DAEMON_SHUTDOWN_COMMAND,
        fence,
        clientNonce,
        serverNonce,
        requestId: 'req-1',
        proof: commandProof,
      }),
    ).toBe(true);
    expect(
      verifyDaemonControlCommand({
        token,
        command: DAEMON_QUIESCE_COMMAND,
        fence,
        clientNonce,
        serverNonce,
        requestId: 'req-1',
        proof: commandProof,
      }),
    ).toBe(false);
    expect(
      verifyDaemonControlCommand({
        token,
        command: DAEMON_SHUTDOWN_COMMAND,
        fence,
        clientNonce,
        serverNonce,
        requestId: 'req-2',
        proof: commandProof,
      }),
    ).toBe(false);

    const payload = JSON.stringify({ ok: true, data: { outcome: 'draining' } });
    const replyProof = signDaemonControlReply({
      token,
      fence,
      clientNonce,
      serverNonce,
      requestId: 'req-1',
      payload,
    });
    expect(
      verifyDaemonControlReply({
        token,
        fence,
        clientNonce,
        serverNonce,
        requestId: 'req-1',
        payload,
        proof: replyProof,
      }),
    ).toBe(true);
    expect(
      verifyDaemonControlReply({
        token,
        fence,
        clientNonce,
        serverNonce,
        requestId: 'req-1',
        payload: '{"ok":true,"data":{"outcome":"tampered"}}',
        proof: replyProof,
      }),
    ).toBe(false);
  });

  it('rejects malformed or wrong-version control frames', () => {
    const clientNonce = createDaemonControlNonce();
    const serverNonce = createDaemonControlNonce();
    const proof = signDaemonControlChallenge({ token, fence, clientNonce, serverNonce });

    expect(parseControlChallengeRequest('not-json')).toBeNull();
    expect(
      parseControlChallengeRequest(
        JSON.stringify({
          type: 'challenge',
          version: 99,
          nonce: clientNonce,
          requestId: 'req-1',
        }),
      ),
    ).toBeNull();
    expect(
      parseControlChallengeRequest(
        JSON.stringify({
          type: 'challenge',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          nonce: clientNonce,
          requestId: 'req-1',
        }),
      ),
    ).toEqual({
      type: 'challenge',
      version: DAEMON_CONTROL_PROTOCOL_VERSION,
      nonce: clientNonce,
      requestId: 'req-1',
    });

    expect(
      parseControlChallengeResponse(
        JSON.stringify({
          type: 'challenge_response',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          serverNonce,
          proof: 'short',
        }),
      ),
    ).toBeNull();
    expect(
      parseControlChallengeResponse(
        JSON.stringify({
          type: 'challenge_response',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          serverNonce,
          proof,
        }),
      ),
    ).toEqual({
      type: 'challenge_response',
      version: DAEMON_CONTROL_PROTOCOL_VERSION,
      serverNonce,
      proof,
    });

    expect(
      parseControlCommandRequest(
        JSON.stringify({
          type: 'command',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          command: 'daemon_other',
          clientNonce,
          serverNonce,
          pid: 1,
          processStartTime: null,
          requestId: 'req-1',
          proof,
        }),
      ),
    ).toBeNull();
    expect(
      parseControlCommandRequest(
        JSON.stringify({
          type: 'command',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          command: DAEMON_SHUTDOWN_COMMAND,
          clientNonce,
          serverNonce,
          pid: 1.5,
          processStartTime: null,
          requestId: 'req-1',
          proof,
        }),
      ),
    ).toBeNull();
    expect(
      parseControlCommandRequest(
        JSON.stringify({
          type: 'command',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          command: DAEMON_SHUTDOWN_COMMAND,
          clientNonce,
          serverNonce,
          pid: 100,
          processStartTime: 'Mon Nov 17 12:00:00 2026',
          requestId: 'req-1',
          proof,
          extra: 'ignored',
        }),
      ),
    ).toEqual({
      type: 'command',
      version: DAEMON_CONTROL_PROTOCOL_VERSION,
      command: DAEMON_SHUTDOWN_COMMAND,
      clientNonce,
      serverNonce,
      pid: 100,
      processStartTime: 'Mon Nov 17 12:00:00 2026',
      requestId: 'req-1',
      proof,
    });

    expect(
      parseControlCommandResponse(
        JSON.stringify({
          type: 'command_response',
          version: DAEMON_CONTROL_PROTOCOL_VERSION,
          payload: 123,
          proof,
        }),
      ),
    ).toBeNull();
  });
});
