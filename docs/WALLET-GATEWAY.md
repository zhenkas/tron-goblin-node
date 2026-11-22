# Wallet HTTP gateway

The gateway exposes a bounded loopback wallet API with an exact origin allowlist. Caller credentials are stripped. Ordinary unsigned builders and signed broadcasts retain their existing upstream behavior.

## Private TronLink Multicall compatibility

TronLink 4.11.2 batches watched TRC20 balances through the fixed address TX56WKxtja91Dybf2FdN4hZbDLyKVxxhAu. A private network normally has no contract there. Enable the read-only compatibility adapter with these gateway configuration fields:

    "tronlink_multicall_enabled": true,
    "tronlink_multicall_profile_file": "/absolute/private/follower-a/private-profile.json"

The private profile generator emits these settings in wallet-gateway.json. Generic installations default to disabled. Each intercepted request rereads the marker, so a fresh private profile at the same path needs no manual genesis edit.

The marker must be a bounded regular file owned by root or the gateway UID, without group/world write access or a leaf symlink. Its schema-1 genesis and profile hash must equal the real upstream genesis block ID and genesis parentHash, which commits the native profile hash. Known public network identities are rejected. The marker is checked again before returning a result.

Only POST /wallet/triggerconstantcontract with that exact address, walletTokensBalance(address[],address), and stock fields contract_address, owner_address, function_selector, parameter, call_value:0 is composed. ABI address padding, offsets, lengths and the 30-token maximum are checked. Extra fields, query parameters, nonzero value and duplicate JSON fields are rejected for this exact call. Other addresses, functions and namespaces retain the ordinary upstream path.

The adapter reads each token's real balanceOf(address), preserving order, duplicates, actual zero and full uint256 values. It returns canonical ABI (uint256[],bool[]). A proven token revert, missing token code or malformed successful token ABI returns zero with that token's error bit set. Transport, node, preflight, timeout or malformed-envelope failures reject the whole batch; they never become successful zero balances.

Canonical head identity is compared before and after each batch. One retry is allowed after head movement; repeated movement fails. Both attempts share one monotonic deadline bounded by the outer request and upstream budgets. Reads are sequential, without additional worker threads, cache or background retries.

This is read composition at the HTTP boundary, not a deployed contract or arbitrary Multicall VM emulation. The outer caller remains the owner of the underlying calls; caller-dependent balanceOf implementations are outside standard token semantics. Native TRX balances use the ordinary account API. The adapter does not build, sign or broadcast a transaction.

## Verification

    python3 -m unittest discover -s scripts -p 'test_wallet_gateway*.py'

Tests use real loopback HTTP servers and synthetic native responses, with no live node, wallet, signing or funding.


## Native TronLink Send preflight

The gateway permits exact POST /wallet/getsignweight as a read. This is used by the
stock SDK to rebuild transaction bytes after adding memo data. GET and the
walletsolidity namespace remain disallowed. Native node JSON/protobuf compatibility
performs the actual permission calculation and returns the full transaction; the
gateway does not invent permission or transaction results.

Ordinary signed-broadcast gateway behavior is unchanged: caller credentials are
stripped and the original body is forwarded once. The web facade adds a narrower
official-extension-only signed JSON broadcast boundary; page-origin writes remain
blocked. No private key or signing service is exposed.
