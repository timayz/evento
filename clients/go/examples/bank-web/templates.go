package main

// HTML templates for the bank-web demo, defined inline to keep the example a
// single copy-pasteable directory. Each page ("index"/"list"/"new"/"view") is a
// full template that composes the shared "head" and "foot" partials.
const templates = `
{{define "head"}}<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>evento bank (Go)</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 760px; margin: 2rem auto; padding: 0 1rem; color: #1a1a1a; }
    a { color: #2563eb; text-decoration: none; }
    a:hover { text-decoration: underline; }
    table { border-collapse: collapse; width: 100%; margin: 1rem 0; }
    th, td { text-align: left; padding: .5rem .75rem; border-bottom: 1px solid #e5e7eb; }
    form { margin: 1rem 0; padding: 1rem; border: 1px solid #e5e7eb; border-radius: 8px; }
    label { display: block; margin: .5rem 0 .2rem; font-size: .9rem; color: #444; }
    input { padding: .4rem; border: 1px solid #cbd5e1; border-radius: 6px; width: 220px; }
    button { margin-top: .8rem; padding: .5rem 1rem; border: 0; border-radius: 6px; background: #2563eb; color: #fff; cursor: pointer; }
    .muted { color: #6b7280; font-size: .9rem; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  </style>
</head>
<body>
  <p><a href="/">Home</a> &middot; <a href="/accounts">Accounts</a> &middot; <a href="/accounts/new">New account</a></p>
{{end}}

{{define "foot"}}</body>
</html>{{end}}

{{define "index"}}{{template "head" .}}
  <h1>evento bank</h1>
  <p>A demo of the <strong>evento Go SDK</strong> talking to <code>evento-server</code> over gRPC.
     Commands write events; the account views are projections replayed from the event log.</p>
  <p><a href="/accounts">Browse accounts &rarr;</a></p>
{{template "foot" .}}{{end}}

{{define "list"}}{{template "head" .}}
  <h1>Accounts</h1>
  {{if .Accounts}}
  <table>
    <thead><tr><th>Owner</th><th>Balance</th><th>Status</th><th></th></tr></thead>
    <tbody>
    {{range .Accounts}}
      <tr>
        <td>{{.OwnerName}}</td>
        <td>{{.Balance}} {{.Currency}}</td>
        <td>{{.Status}}</td>
        <td><a href="/accounts/{{.ID}}">view</a></td>
      </tr>
    {{end}}
    </tbody>
  </table>
  {{else}}
  <p class="muted">No accounts yet. <a href="/accounts/new">Open one</a>.</p>
  {{end}}
{{template "foot" .}}{{end}}

{{define "new"}}{{template "head" .}}
  <h1>New account</h1>
  <form method="post" action="/accounts">
    <label>Owner name</label>
    <input name="owner_name" required>
    <label>Currency</label>
    <input name="currency" value="USD" required>
    <label>Initial balance</label>
    <input name="initial_balance" type="number" value="0" min="0">
    <div><button type="submit">Open account</button></div>
  </form>
{{template "foot" .}}{{end}}

{{define "view"}}{{template "head" .}}
  {{with .Account}}
  <h1>{{.OwnerName}}</h1>
  <p class="muted">{{.ID}}</p>
  <p><strong>Balance:</strong> {{.Balance}} {{.Currency}} &middot;
     <strong>Status:</strong> {{.Status}} &middot;
     <strong>Version:</strong> {{.Version}}</p>

  <div class="grid">
    <form method="post" action="/accounts/{{.ID}}/deposit">
      <strong>Deposit</strong>
      <label>Amount</label>
      <input name="amount" type="number" min="1">
      <div><button type="submit">Deposit</button></div>
    </form>

    <form method="post" action="/accounts/{{.ID}}/withdraw">
      <strong>Withdraw</strong>
      <label>Amount</label>
      <input name="amount" type="number" min="1">
      <div><button type="submit">Withdraw</button></div>
    </form>
  </div>

  <form method="post" action="/accounts/{{.ID}}/transfer">
    <strong>Transfer</strong>
    <label>To account</label>
    <select name="to_account_id" style="padding:.4rem;border:1px solid #cbd5e1;border-radius:6px;width:240px">
      {{$self := .ID}}
      {{range $.Accounts}}{{if ne .ID $self}}<option value="{{.ID}}">{{.OwnerName}} ({{.Balance}} {{.Currency}})</option>{{end}}{{end}}
    </select>
    <label>Amount</label>
    <input name="amount" type="number" min="1">
    <div><button type="submit">Transfer</button></div>
  </form>
  {{end}}
{{template "foot" .}}{{end}}
`
