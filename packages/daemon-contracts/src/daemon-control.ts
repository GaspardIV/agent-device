import crypto from 'node:crypto';
import { timingSafeStringEqual } from '@agent-device/host-kit/transport';
import { isRecord } from '@agent-device/kernel/record';
import {
  DAEMON_QUIESCE_COMMAND,
  DAEMON_SHUTDOWN_COMMAND,
  type DaemonLifecycleFence,
} from './daemon-resolution.ts';

export { DAEMON_QUIESCE_COMMAND, DAEMON_SHUTDOWN_COMMAND };

export const DAEMON_CONTROL_PROTOCOL_VERSION = 1;
export const DAEMON_CONTROL_MAX_LINE_BYTES = 64 * 1024;

export type DaemonControlCommand = typeof DAEMON_QUIESCE_COMMAND | typeof DAEMON_SHUTDOWN_COMMAND;

export type DaemonControlChallengeRequest = Readonly<{
  type: 'challenge';
  version: typeof DAEMON_CONTROL_PROTOCOL_VERSION;
  nonce: string;
  requestId: string;
}>;

export type DaemonControlChallengeResponse = Readonly<{
  type: 'challenge_response';
  version: typeof DAEMON_CONTROL_PROTOCOL_VERSION;
  serverNonce: string;
  proof: string;
}>;

export type DaemonControlCommandRequest = Readonly<{
  type: 'command';
  version: typeof DAEMON_CONTROL_PROTOCOL_VERSION;
  command: DaemonControlCommand;
  clientNonce: string;
  serverNonce: string;
  pid: number;
  processStartTime: string | null;
  requestId: string;
  proof: string;
}>;

export type DaemonControlCommandResponse = Readonly<{
  type: 'command_response';
  version: typeof DAEMON_CONTROL_PROTOCOL_VERSION;
  payload: string;
  proof: string;
}>;

const NONCE_PATTERN = /^[0-9a-f]{32,128}$/i;
const PROOF_PATTERN = /^[0-9a-f]{64}$/i;

export function createDaemonControlNonce(): string {
  return crypto.randomBytes(16).toString('hex');
}

export function isValidControlNonce(value: unknown): value is string {
  return typeof value === 'string' && NONCE_PATTERN.test(value);
}

function isValidControlProof(value: unknown): value is string {
  return typeof value === 'string' && PROOF_PATTERN.test(value);
}

function isDaemonControlCommand(value: unknown): value is DaemonControlCommand {
  return value === DAEMON_QUIESCE_COMMAND || value === DAEMON_SHUTDOWN_COMMAND;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function computeControlHmac(token: string, fields: readonly (string | number)[]): string {
  return crypto.createHmac('sha256', token).update(JSON.stringify(fields)).digest('hex');
}

function verifyControlHmac(
  token: string,
  fields: readonly (string | number)[],
  proof: string,
): boolean {
  if (!isValidControlProof(proof)) return false;
  return timingSafeStringEqual(computeControlHmac(token, fields), proof.toLowerCase());
}

export type ControlChallengeProofParams = Readonly<{
  token: string;
  fence: DaemonLifecycleFence;
  clientNonce: string;
  serverNonce: string;
}>;

function challengeProofFields(params: ControlChallengeProofParams): readonly (string | number)[] {
  return [
    'agent-device.control.challenge.v1',
    params.fence.pid,
    params.fence.processStartTime ?? '',
    params.clientNonce,
    params.serverNonce,
  ];
}

export function signDaemonControlChallenge(params: ControlChallengeProofParams): string {
  return computeControlHmac(params.token, challengeProofFields(params));
}

export function verifyDaemonControlChallenge(
  params: ControlChallengeProofParams & Readonly<{ proof: string }>,
): boolean {
  return verifyControlHmac(params.token, challengeProofFields(params), params.proof);
}

export type ControlCommandProofParams = Readonly<{
  token: string;
  command: DaemonControlCommand;
  fence: DaemonLifecycleFence;
  clientNonce: string;
  serverNonce: string;
  requestId: string;
}>;

function commandProofFields(params: ControlCommandProofParams): readonly (string | number)[] {
  return [
    'agent-device.control.command.v1',
    params.command,
    params.fence.pid,
    params.fence.processStartTime ?? '',
    params.clientNonce,
    params.serverNonce,
    params.requestId,
  ];
}

export function signDaemonControlCommand(params: ControlCommandProofParams): string {
  return computeControlHmac(params.token, commandProofFields(params));
}

export function verifyDaemonControlCommand(
  params: ControlCommandProofParams & Readonly<{ proof: string }>,
): boolean {
  return verifyControlHmac(params.token, commandProofFields(params), params.proof);
}

export type ControlReplyProofParams = Readonly<{
  token: string;
  fence: DaemonLifecycleFence;
  clientNonce: string;
  serverNonce: string;
  requestId: string;
  payload: string;
}>;

function replyProofFields(params: ControlReplyProofParams): readonly (string | number)[] {
  return [
    'agent-device.control.reply.v1',
    params.fence.pid,
    params.fence.processStartTime ?? '',
    params.clientNonce,
    params.serverNonce,
    params.requestId,
    params.payload,
  ];
}

export function signDaemonControlReply(params: ControlReplyProofParams): string {
  return computeControlHmac(params.token, replyProofFields(params));
}

export function verifyDaemonControlReply(
  params: ControlReplyProofParams & Readonly<{ proof: string }>,
): boolean {
  return verifyControlHmac(params.token, replyProofFields(params), params.proof);
}

function parseControlJsonRecord(
  line: string,
  expectedType: string,
): Readonly<Record<string, unknown>> | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (!isRecord(parsed)) return null;
  if (parsed.type !== expectedType || parsed.version !== DAEMON_CONTROL_PROTOCOL_VERSION) {
    return null;
  }
  return parsed;
}

export function parseControlChallengeRequest(line: string): DaemonControlChallengeRequest | null {
  const parsed = parseControlJsonRecord(line, 'challenge');
  if (!parsed || !isValidControlNonce(parsed.nonce) || !isNonEmptyString(parsed.requestId)) {
    return null;
  }
  return {
    type: 'challenge',
    version: DAEMON_CONTROL_PROTOCOL_VERSION,
    nonce: parsed.nonce,
    requestId: parsed.requestId,
  };
}

export function parseControlChallengeResponse(line: string): DaemonControlChallengeResponse | null {
  const parsed = parseControlJsonRecord(line, 'challenge_response');
  if (!parsed || !isValidControlNonce(parsed.serverNonce) || !isValidControlProof(parsed.proof)) {
    return null;
  }
  return {
    type: 'challenge_response',
    version: DAEMON_CONTROL_PROTOCOL_VERSION,
    serverNonce: parsed.serverNonce,
    proof: parsed.proof,
  };
}

export function parseControlCommandRequest(line: string): DaemonControlCommandRequest | null {
  const parsed = parseControlJsonRecord(line, 'command');
  if (!parsed) return null;
  if (
    !isDaemonControlCommand(parsed.command) ||
    !isValidControlNonce(parsed.clientNonce) ||
    !isValidControlNonce(parsed.serverNonce) ||
    typeof parsed.pid !== 'number' ||
    !Number.isInteger(parsed.pid) ||
    (parsed.processStartTime !== null && typeof parsed.processStartTime !== 'string') ||
    !isNonEmptyString(parsed.requestId) ||
    !isValidControlProof(parsed.proof)
  ) {
    return null;
  }
  return {
    type: 'command',
    version: DAEMON_CONTROL_PROTOCOL_VERSION,
    command: parsed.command,
    clientNonce: parsed.clientNonce,
    serverNonce: parsed.serverNonce,
    pid: parsed.pid,
    processStartTime: parsed.processStartTime,
    requestId: parsed.requestId,
    proof: parsed.proof,
  };
}

export function parseControlCommandResponse(line: string): DaemonControlCommandResponse | null {
  const parsed = parseControlJsonRecord(line, 'command_response');
  if (!parsed || typeof parsed.payload !== 'string' || !isValidControlProof(parsed.proof)) {
    return null;
  }
  return {
    type: 'command_response',
    version: DAEMON_CONTROL_PROTOCOL_VERSION,
    payload: parsed.payload,
    proof: parsed.proof,
  };
}
