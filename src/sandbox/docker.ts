import { spawn } from 'node:child_process';
import crypto from 'node:crypto';

export interface SandboxOptions {
  image?: string;
  command?: string[];
  input?: unknown;
  timeoutMs?: number;
  cpus?: string;
  memory?: string;
  memorySwap?: string;
  pidsLimit?: number;
  network?: string;
  readOnly?: boolean;
  tmpfs?: string;
}

export class SandboxTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SandboxTimeoutError';
  }
}

export class SandboxExecutionError extends Error {
  public exitCode: number | null;
  public stderr: string;

  constructor(message: string, exitCode: number | null, stderr: string) {
    super(message);
    this.name = 'SandboxExecutionError';
    this.exitCode = exitCode;
    this.stderr = stderr;
  }
}

export class DockerSandboxRunner {
  /**
   * Executes arbitrary user code in a strictly isolated ephemeral Docker container.
   * Enforces:
   *  - CPU capping (--cpus)
   *  - Memory limits and swap disabling (--memory, --memory-swap)
   *  - Process count capping to mitigate fork-bombs (--pids-limit)
   *  - Total network isolation (--network=none)
   *  - Read-only root filesystem (--read-only)
   *  - Size-capped tmpfs scratch space (--tmpfs)
   *  - Hard timeout with guaranteed `docker kill` abort
   */
  public async execute<T>(options: SandboxOptions): Promise<T> {
    const containerName = `stepforge-sbx-${crypto.randomUUID().slice(0, 12)}`;
    const timeoutMs = options.timeoutMs ?? 5000;
    const image = options.image ?? 'node:20-alpine';

    const args: string[] = [
      'run',
      '--name',
      containerName,
      '--rm',
      '-i',
      `--cpus=${options.cpus ?? '1.0'}`,
      `--memory=${options.memory ?? '256m'}`,
      `--memory-swap=${options.memorySwap ?? '256m'}`,
      `--pids-limit=${options.pidsLimit ?? 64}`,
      `--network=${options.network ?? 'none'}`,
    ];

    if (options.readOnly !== false) {
      args.push('--read-only');
    }

    if (options.tmpfs) {
      args.push('--tmpfs', options.tmpfs);
    } else {
      args.push('--tmpfs', '/tmp:rw,noexec,nosuid,size=64m');
    }

    args.push(image);

    if (options.command && options.command.length > 0) {
      args.push(...options.command);
    }

    return new Promise<T>((resolve, reject) => {
      let isSettled = false;
      let stdout = '';
      let stderr = '';

      const child = spawn('docker', args, {
        stdio: ['pipe', 'pipe', 'pipe'],
      });

      // Hard timeout timer
      const timeoutTimer = setTimeout(() => {
        if (isSettled) return;
        isSettled = true;

        // Force kill container via docker daemon
        spawn('docker', ['kill', containerName], { stdio: 'ignore' });
        try {
          child.kill('SIGKILL');
        } catch {}

        reject(
          new SandboxTimeoutError(
            `Sandbox container "${containerName}" exceeded hard timeout of ${timeoutMs}ms and was killed.`
          )
        );
      }, timeoutMs);

      child.stdout?.on('data', (chunk) => {
        stdout += chunk.toString();
      });

      child.stderr?.on('data', (chunk) => {
        stderr += chunk.toString();
      });

      child.on('error', (err) => {
        if (isSettled) return;
        isSettled = true;
        clearTimeout(timeoutTimer);
        reject(err);
      });

      child.on('close', (code) => {
        if (isSettled) return;
        isSettled = true;
        clearTimeout(timeoutTimer);

        if (code !== 0) {
          return reject(
            new SandboxExecutionError(
              `Sandbox container failed with exit code ${code}: ${stderr.trim()}`,
              code,
              stderr
            )
          );
        }

        const trimmedOut = stdout.trim();
        if (!trimmedOut) {
          return resolve(null as unknown as T);
        }

        try {
          const parsed = JSON.parse(trimmedOut);
          resolve(parsed as T);
        } catch {
          // If not JSON, resolve as raw string
          resolve(trimmedOut as unknown as T);
        }
      });

      // Send input payload via STDIN
      if (options.input !== undefined) {
        const payloadStr =
          typeof options.input === 'string'
            ? options.input
            : JSON.stringify(options.input);
        child.stdin?.write(payloadStr);
      }
      child.stdin?.end();
    });
  }
}
