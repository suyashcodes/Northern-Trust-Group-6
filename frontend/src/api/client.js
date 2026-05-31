const BASE_URL = `${import.meta.env.VITE_API_URL || 'http://localhost:4000'}/api/v1`;

async function request(path, options = {}) {
  const url = `${BASE_URL}${path}`;
  const config = {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  };

  try {
    const response = await fetch(url, config);
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: response.statusText }));
      throw new Error(error.message || `Request failed: ${response.status}`);
    }
    return await response.json();
  } catch (err) {
    if (err.name === 'TypeError' && err.message.includes('fetch')) {
      throw new Error('Cannot connect to orchestrator API. Ensure the backend is running on port 4000.');
    }
    throw err;
  }
}

// ─── Workflow Endpoints ────────────────────────────────────────

export async function startWorkflow(input) {
  return request('/workflows/start', {
    method: 'POST',
    body: JSON.stringify(input),
  });
}

export async function listWorkflows(params = {}) {
  const query = new URLSearchParams();
  if (params.state) query.set('state', params.state);
  if (params.limit) query.set('limit', params.limit);
  const qs = query.toString();
  return request(`/workflows${qs ? `?${qs}` : ''}`);
}

export async function getWorkflow(id) {
  return request(`/workflows/${id}`);
}

export async function getWorkflowLogs(id) {
  return request(`/workflows/${id}/logs`);
}

export async function pauseWorkflow(id) {
  return request(`/workflows/${id}/pause`, { method: 'POST' });
}

export async function resumeWorkflow(id) {
  return request(`/workflows/${id}/resume`, { method: 'POST' });
}

export async function terminateWorkflow(id) {
  return request(`/workflows/${id}/terminate`, { method: 'POST' });
}

// ─── Task Endpoints ────────────────────────────────────────────

export async function retryTask(id) {
  return request(`/tasks/${id}/retry`, { method: 'POST' });
}

// ─── Human Task Endpoints ──────────────────────────────────────

export async function listHumanTasks() {
  return request('/human-tasks');
}

export async function approveHumanTask(id, decidedBy) {
  return request(`/human-tasks/${id}/approve`, {
    method: 'POST',
    body: JSON.stringify({ decided_by: decidedBy }),
  });
}

export async function rejectHumanTask(id, reason) {
  return request(`/human-tasks/${id}/reject`, {
    method: 'POST',
    body: JSON.stringify({ reason }),
  });
}
