/**
 * MIT License
 *
 * Copyright (c) 2026 Aparavi Software AG
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import { DAPClient } from '../src/client/core/DAPClient';

const TRACEPARENT = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';

function client(traceparent?: () => string | undefined): DAPClient {
	return new DAPClient('TEST', undefined, { traceparent });
}

describe('traceparent propagation', () => {
	it('adds the provider traceparent to every request', () => {
		const c = client(() => TRACEPARENT);
		expect(c.buildRequest('execute').traceparent).toBe(TRACEPARENT);
		expect(c.buildRequest('status').traceparent).toBe(TRACEPARENT);
	});

	it('sends nothing without a provider', () => {
		expect('traceparent' in client().buildRequest('execute')).toBe(false);
	});

	it('drops a malformed or missing value', () => {
		for (const value of ['garbage', '00-short-01', '', undefined]) {
			expect('traceparent' in client(() => value).buildRequest('execute')).toBe(false);
		}
	});

	it('never lets a failing provider break a request', () => {
		const c = client(() => {
			throw new Error('tracer not ready');
		});
		const request = c.buildRequest('execute');
		expect(request.command).toBe('execute');
		expect('traceparent' in request).toBe(false);
	});
});
