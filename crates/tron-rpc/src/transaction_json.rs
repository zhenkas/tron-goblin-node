//! Canonical JSON decoding for read-only transaction permission queries.
//!
//! This is deliberately separate from signed broadcast parsing. TronWeb's
//! addUpdateData edits raw_data.data while leaving cached raw_data_hex/txID
//! stale, then asks getSignWeight to recalculate them. JSON raw_data is the
//! authoritative source here; no transaction is signed, built from node state,
//! admitted, or broadcast by this module.
use prost::Message as _;
use serde_json::{json, Map, Value};
use tron_proto::transaction::{contract::ContractType, Contract, Raw};
use tron_proto::Transaction;

use crate::methods::RpcError;

const MAX_TRANSACTION_BYTES: usize = 500_000;
const MAX_SIGNATURES: usize = 100;

fn bad(message: &str) -> RpcError {
    RpcError::invalid_params(format!("permission transaction: {message}"))
}
fn object<'a>(value: &'a Value, allowed: &[&str]) -> Result<&'a Map<String, Value>, RpcError> {
    let map = value.as_object().ok_or_else(|| bad("expected object"))?;
    if map.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(bad("unsupported field"));
    }
    Ok(map)
}
fn integer(map: &Map<String, Value>, key: &str, required: bool) -> Result<i64, RpcError> {
    let Some(value) = map.get(key) else {
        return if required {
            Err(bad("missing integer"))
        } else {
            Ok(0)
        };
    };
    let number = if let Some(n) = value.as_i64() {
        n
    } else if let Some(s) = value.as_str() {
        if s.is_empty()
            || (s.len() > 1 && s.starts_with('0'))
            || !s.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(bad("expected canonical nonnegative int64"));
        }
        s.parse::<i64>().map_err(|_| bad("integer outside int64"))?
    } else {
        return Err(bad("expected canonical nonnegative int64"));
    };
    if number < 0 {
        return Err(bad("negative integer"));
    }
    Ok(number)
}
fn bytes(value: &Value) -> Result<Vec<u8>, RpcError> {
    let text = value.as_str().ok_or_else(|| bad("expected hex bytes"))?;
    let text = text.strip_prefix("0x").unwrap_or(text);
    if text.len() > MAX_TRANSACTION_BYTES * 2 || text.len() % 2 != 0 {
        return Err(bad("invalid hex length"));
    }
    hex::decode(text).map_err(|_| bad("invalid hex bytes"))
}
fn field_bytes(map: &Map<String, Value>, key: &str, required: bool) -> Result<Vec<u8>, RpcError> {
    match map.get(key) {
        Some(value) => bytes(value),
        None if !required => Ok(Vec::new()),
        None => Err(bad("missing bytes")),
    }
}
fn address(map: &Map<String, Value>, key: &str) -> Result<Vec<u8>, RpcError> {
    let value = map.get(key).ok_or_else(|| bad("missing address"))?;
    let text = value
        .as_str()
        .ok_or_else(|| bad("expected address string"))?;
    let decoded = if text.starts_with('T') {
        tron_crypto::base58check::decode_address(text)
            .map_err(|_| bad("invalid Base58 address"))?
            .as_bytes()
            .to_vec()
    } else {
        bytes(value)?
    };
    if decoded.len() != 21 || decoded[0] != 0x41 {
        return Err(bad("invalid TRON address"));
    }
    Ok(decoded)
}

fn contract(value: &Value) -> Result<Contract, RpcError> {
    let map = object(
        value,
        &[
            "type",
            "parameter",
            "Permission_id",
            "provider",
            "ContractName",
        ],
    )?;
    let kind = map
        .get("type")
        .and_then(Value::as_str)
        .and_then(ContractType::from_str_name)
        .ok_or_else(|| bad("invalid contract type"))?;
    let parameter = object(
        map.get("parameter")
            .ok_or_else(|| bad("missing contract parameter"))?,
        &["type_url", "value"],
    )?;
    let expected_url = format!("type.googleapis.com/protocol.{}", kind.as_str_name());
    if parameter.get("type_url").and_then(Value::as_str) != Some(expected_url.as_str()) {
        return Err(bad("contract type_url differs from type"));
    }
    let value = parameter
        .get("value")
        .ok_or_else(|| bad("missing contract value"))?;
    // These are the stock wallet send and contract-call messages. Other
    // contract types remain available through the original full-wire hex API.
    let encoded = match kind {
        ContractType::TransferContract => {
            let m = object(value, &["owner_address", "to_address", "amount"])?;
            tron_proto::TransferContract {
                owner_address: address(m, "owner_address")?,
                to_address: address(m, "to_address")?,
                amount: integer(m, "amount", true)?,
            }
            .encode_to_vec()
        }
        ContractType::TransferAssetContract => {
            let m = object(
                value,
                &["owner_address", "to_address", "asset_name", "amount"],
            )?;
            tron_proto::TransferAssetContract {
                owner_address: address(m, "owner_address")?,
                to_address: address(m, "to_address")?,
                asset_name: field_bytes(m, "asset_name", true)?,
                amount: integer(m, "amount", true)?,
            }
            .encode_to_vec()
        }
        ContractType::TriggerSmartContract => {
            let m = object(
                value,
                &[
                    "owner_address",
                    "contract_address",
                    "call_value",
                    "data",
                    "call_token_value",
                    "token_id",
                ],
            )?;
            tron_proto::TriggerSmartContract {
                owner_address: address(m, "owner_address")?,
                contract_address: address(m, "contract_address")?,
                call_value: integer(m, "call_value", false)?,
                data: field_bytes(m, "data", false)?,
                call_token_value: integer(m, "call_token_value", false)?,
                token_id: integer(m, "token_id", false)?,
            }
            .encode_to_vec()
        }
        _ => return Err(bad("contract type requires full-wire hex input")),
    };
    let permission = integer(map, "Permission_id", false)?;
    Ok(Contract {
        r#type: kind as i32,
        parameter: Some(prost_types::Any {
            type_url: expected_url,
            value: encoded,
        }),
        permission_id: i32::try_from(permission).map_err(|_| bad("Permission_id outside int32"))?,
        provider: field_bytes(map, "provider", false)?,
        contract_name: field_bytes(map, "ContractName", false)?,
    })
}

/// Decode the current raw_data JSON, intentionally not cached txID/raw_data_hex.
pub(crate) fn from_json(value: &Value) -> Result<Transaction, RpcError> {
    let top = object(
        value,
        &[
            "raw_data",
            "raw_data_hex",
            "txID",
            "signature",
            "visible",
            "ret",
        ],
    )?;
    if top.get("visible").is_some_and(|v| !v.is_boolean()) {
        return Err(bad("visible must be boolean"));
    }
    // Even stale cache fields must be well formed; their content is not used.
    if let Some(hex) = top.get("raw_data_hex") {
        let _ = bytes(hex)?;
    }
    if let Some(id) = top.get("txID") {
        if bytes(id)?.len() != 32 {
            return Err(bad("txID must contain 32 bytes"));
        }
    }
    if top
        .get("ret")
        .is_some_and(|v| v.as_array().is_none_or(|a| !a.is_empty()))
    {
        return Err(bad("execution results are not permission-query input"));
    }
    let raw = object(
        top.get("raw_data").ok_or_else(|| bad("missing raw_data"))?,
        &[
            "ref_block_bytes",
            "ref_block_num",
            "ref_block_hash",
            "expiration",
            "data",
            "contract",
            "scripts",
            "timestamp",
            "fee_limit",
            "auths",
        ],
    )?;
    if raw
        .get("auths")
        .is_some_and(|v| v.as_array().is_none_or(|a| !a.is_empty()))
    {
        return Err(bad("raw_data.auths requires full-wire hex input"));
    }
    let contracts = raw
        .get("contract")
        .and_then(Value::as_array)
        .ok_or_else(|| bad("missing contract array"))?;
    if contracts.len() != 1 {
        return Err(bad("exactly one contract is required"));
    }
    let ref_bytes = field_bytes(raw, "ref_block_bytes", true)?;
    let ref_hash = field_bytes(raw, "ref_block_hash", true)?;
    if ref_bytes.len() != 2 || ref_hash.len() != 8 {
        return Err(bad("invalid TAPOS byte lengths"));
    }
    let raw = Raw {
        ref_block_bytes: ref_bytes,
        ref_block_num: integer(raw, "ref_block_num", false)?,
        ref_block_hash: ref_hash,
        expiration: integer(raw, "expiration", true)?,
        auths: Vec::new(),
        data: field_bytes(raw, "data", false)?,
        contract: vec![contract(&contracts[0])?],
        scripts: field_bytes(raw, "scripts", false)?,
        timestamp: integer(raw, "timestamp", true)?,
        fee_limit: integer(raw, "fee_limit", false)?,
    };
    let signatures = match top.get("signature") {
        None => Vec::new(),
        Some(value) => {
            let values = value
                .as_array()
                .ok_or_else(|| bad("signature must be an array"))?;
            if values.len() > MAX_SIGNATURES {
                return Err(bad("too many signatures"));
            }
            values
                .iter()
                .map(|v| {
                    let signature = bytes(v)?;
                    if signature.len() != 65 {
                        return Err(bad("signature must contain 65 bytes"));
                    }
                    Ok(signature)
                })
                .collect::<Result<Vec<_>, RpcError>>()?
        }
    };
    let tx = Transaction {
        raw_data: Some(raw),
        signature: signatures,
        ..Default::default()
    };
    if tx.encoded_len() > MAX_TRANSACTION_BYTES {
        return Err(bad("transaction is too large"));
    }
    Ok(tx)
}

// Recover the submitted raw_data span for legacy full-wire callers. Unknown
// fields in that span must remain in raw_data_hex and in the displayed txID.
fn raw_span(bytes: &[u8]) -> Option<&[u8]> {
    fn varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
        let mut out = 0u64;
        for shift in (0..70).step_by(7) {
            let b = *bytes.get(*at)?;
            *at += 1;
            if shift == 63 && b > 1 {
                return None;
            }
            out |= u64::from(b & 127) << shift;
            if b < 128 {
                return Some(out);
            }
        }
        None
    }
    let mut at = 0usize;
    let mut span = None;
    while at < bytes.len() {
        let tag = varint(bytes, &mut at)?;
        match tag & 7 {
            0 => {
                varint(bytes, &mut at)?;
            }
            1 => at = at.checked_add(8)?,
            5 => at = at.checked_add(4)?,
            2 => {
                let length = usize::try_from(varint(bytes, &mut at)?).ok()?;
                let end = at.checked_add(length)?;
                if end > bytes.len() {
                    return None;
                }
                if tag >> 3 == 1 {
                    if span.is_some() {
                        return None;
                    }
                    span = Some(&bytes[at..end]);
                }
                at = end;
            }
            _ => return None,
        }
        if at > bytes.len() {
            return None;
        }
    }
    span
}

/// Full java-shaped TransactionExtention, with legacy summary members retained.
pub(crate) fn extension(
    tx: &Transaction,
    wire: &[u8],
    tx_id: &[u8; 32],
    visible: bool,
) -> Result<Value, RpcError> {
    let raw = tx
        .raw_data
        .as_ref()
        .ok_or_else(|| bad("missing raw_data"))?;
    let encoded = raw.encode_to_vec();
    let raw_bytes = raw_span(wire).unwrap_or(&encoded);
    let mut raw_json = crate::http_rest::format_raw_data_for_http(raw);
    if raw.ref_block_num != 0 {
        raw_json["ref_block_num"] = json!(raw.ref_block_num);
    }
    if !raw.scripts.is_empty() {
        raw_json["scripts"] = json!(hex::encode(&raw.scripts));
    }
    if let Some(contracts) = raw_json["contract"].as_array_mut() {
        for (json, proto) in contracts.iter_mut().zip(&raw.contract) {
            if !proto.provider.is_empty() {
                json["provider"] = json!(hex::encode(&proto.provider));
            }
            if !proto.contract_name.is_empty() {
                json["ContractName"] = json!(hex::encode(&proto.contract_name));
            }
        }
    }
    let mut transaction = json!({
        "visible": visible, "txID": hex::encode(tx_id), "raw_data": raw_json,
        "raw_data_hex": hex::encode(raw_bytes),
    });
    if !tx.signature.is_empty() {
        transaction["signature"] = json!(tx.signature.iter().map(hex::encode).collect::<Vec<_>>());
    }
    Ok(json!({
        "transaction": transaction,
        "txid": hex::encode(tx_id),
        "result": {"result": true},
        // Keep the old JSON-RPC summary fields for existing full-wire callers.
        "txID": format!("0x{}", hex::encode(tx_id)),
        "signature_count": tx.signature.len(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{methods, RpcState};
    use std::sync::Arc;
    use tron_chainbase::{KvBackend, MemBackend};
    use tron_crypto::address::Address;

    fn owner() -> String {
        format!("41{}", "11".repeat(20))
    }
    fn fixture() -> Value {
        json!({
            "visible":false,
            "raw_data":{
                "ref_block_bytes":"0001", "ref_block_hash":"0000000000000000",
                "expiration":60001,"timestamp":1,
                "contract":[{
                    "type":"TransferContract",
                    "parameter":{
                        "type_url":"type.googleapis.com/protocol.TransferContract",
                        "value":{"owner_address":owner(),
                                 "to_address":format!("41{}", "22".repeat(20)),
                                 "amount":10000000000_i64}
                    }
                }]
            }
        })
    }
    fn state(owner: &[u8]) -> RpcState {
        fn mem() -> Arc<dyn KvBackend> {
            Arc::new(MemBackend::new())
        }
        let state = RpcState::new(mem(), mem(), mem(), mem(), mem(), 11_111);
        let address = Address::from_raw(owner.try_into().unwrap());
        state
            .accounts
            .put(
                &address,
                &tron_proto::Account {
                    address: owner.to_vec(),
                    balance: 1_000_000,
                    ..Default::default()
                },
            )
            .unwrap();
        state
    }
    fn query(value: Value) -> Value {
        let owner = hex::decode(owner()).unwrap();
        methods::get_sign_weight(&json!([value]), &state(&owner)).unwrap()
    }
    #[test]
    fn unsigned_json_returns_real_weight_and_complete_extension() {
        let f = fixture();
        let expected = from_json(&f).unwrap();
        let response = query(f);
        assert_eq!(response["current_weight"], 0);
        assert_eq!(response["permission"]["threshold"], 1);
        assert_eq!(response["result"]["code"], "NOT_ENOUGH_PERMISSION");
        let tx = &response["transaction"]["transaction"];
        assert_eq!(
            tx["raw_data_hex"],
            hex::encode(expected.raw_data.as_ref().unwrap().encode_to_vec())
        );
        assert_eq!(
            tx["txID"],
            hex::encode(tron_types::tx_id(&expected).unwrap())
        );
        assert!(tx.get("signature").is_none());
        assert_eq!(
            tx["raw_data"]["contract"][0]["parameter"]["value"]["amount"],
            10000000000_i64
        );
        assert_eq!(response["transaction"]["result"]["result"], true);
        assert_eq!(response["transaction"]["signature_count"], 0);
    }
    #[test]
    fn memo_json_replaces_stale_unsigned_cache_without_changing_other_terms() {
        let mut f = fixture();
        let old = from_json(&f).unwrap();
        let old_id = tron_types::tx_id(&old).unwrap();
        f["raw_data_hex"] = json!(hex::encode(old.raw_data.as_ref().unwrap().encode_to_vec()));
        f["txID"] = json!(hex::encode(old_id));
        f["raw_data"]["data"] = json!(hex::encode("new memo"));
        f["raw_data"]["fee_limit"] = json!("30000000");
        f["raw_data"]["contract"][0]["Permission_id"] = json!(0);
        let response = query(f.clone());
        let tx = &response["transaction"]["transaction"];
        let expected = from_json(&f).unwrap();
        assert_ne!(tx["txID"], hex::encode(old_id));
        assert_eq!(
            tx["txID"],
            hex::encode(tron_types::tx_id(&expected).unwrap())
        );
        assert_eq!(tx["raw_data"]["data"], hex::encode("new memo"));
        assert_eq!(tx["raw_data"]["fee_limit"], 30000000);
        assert_eq!(tx["raw_data"]["ref_block_bytes"], "0001");
        assert_eq!(tx["raw_data"]["timestamp"], 1);
        assert_eq!(tx["raw_data"]["expiration"], 60001);
        assert_eq!(from_json(tx).unwrap().raw_data, expected.raw_data);
    }
    #[test]
    fn permission_id_and_all_proto_raw_terms_are_preserved() {
        let mut f = fixture();
        f["raw_data"]["contract"][0]["Permission_id"] = json!("2");
        f["raw_data"]["ref_block_num"] = json!("123");
        f["raw_data"]["scripts"] = json!("00ff");
        f["raw_data"]["data"] = json!("");
        f["raw_data"]["contract"][0]["provider"] = json!("abcd");
        f["raw_data"]["contract"][0]["ContractName"] = json!("74657374");
        let tx = from_json(&f).unwrap();
        assert_eq!(tx.raw_data.as_ref().unwrap().contract[0].permission_id, 2);
        let id = tron_types::tx_id(&tx).unwrap();
        let ext = extension(&tx, &tx.encode_to_vec(), &id, false).unwrap();
        assert_eq!(
            ext["transaction"]["raw_data"]["contract"][0]["Permission_id"],
            2
        );
        assert_eq!(from_json(&ext["transaction"]).unwrap(), tx);
    }
    #[test]
    fn typed_transfer_asset_and_trigger_messages_encode_real_proto() {
        let mut f = fixture();
        f["raw_data"]["contract"][0] = json!({"type":"TransferAssetContract","parameter":{
            "type_url":"type.googleapis.com/protocol.TransferAssetContract",
            "value":{"owner_address":owner(),"to_address":owner(),"asset_name":"31303030303031","amount":"42"}
        }});
        let tx = from_json(&f).unwrap();
        let param = tx.raw_data.unwrap().contract[0]
            .parameter
            .as_ref()
            .unwrap()
            .value
            .clone();
        let parsed = tron_proto::TransferAssetContract::decode(param.as_slice()).unwrap();
        assert_eq!(parsed.asset_name, b"1000001");
        assert_eq!(parsed.amount, 42);
        f["raw_data"]["contract"][0] = json!({"type":"TriggerSmartContract","parameter":{
            "type_url":"type.googleapis.com/protocol.TriggerSmartContract",
            "value":{"owner_address":owner(),"contract_address":owner(),"data":"a9059cbb",
                     "call_value":"10","call_token_value":20,"token_id":1000001}
        }});
        let tx = from_json(&f).unwrap();
        let param = tx.raw_data.unwrap().contract[0]
            .parameter
            .as_ref()
            .unwrap()
            .value
            .clone();
        let parsed = tron_proto::TriggerSmartContract::decode(param.as_slice()).unwrap();
        assert_eq!(parsed.data, hex::decode("a9059cbb").unwrap());
        assert_eq!(
            (parsed.call_value, parsed.call_token_value, parsed.token_id),
            (10, 20, 1000001)
        );
    }
    #[test]
    fn invalid_json_is_rejected_instead_of_dropping_terms() {
        let mut cases = Vec::new();
        for value in [
            json!("01"),
            json!("1e3"),
            json!(-1),
            json!(1.5),
            json!(true),
            json!("9223372036854775808"),
        ] {
            let mut f = fixture();
            f["raw_data"]["contract"][0]["parameter"]["value"]["amount"] = value;
            cases.push(f);
        }
        for value in [json!("00"), json!("zz"), json!(null)] {
            let mut f = fixture();
            f["raw_data"]["ref_block_hash"] = value;
            cases.push(f);
        }
        let mut f = fixture();
        f["raw_data"]["contract"][0]["parameter"]["type_url"] =
            json!("type.googleapis.com/protocol.TriggerSmartContract");
        cases.push(f);
        let mut f = fixture();
        f["raw_data"]["contract"][0]["parameter"]["value"]["unknown"] = json!(1);
        cases.push(f);
        let mut f = fixture();
        f["raw_data"]["unknown"] = json!(1);
        cases.push(f);
        let mut f = fixture();
        f["raw_data"]["contract"] = json!([]);
        cases.push(f);
        let mut f = fixture();
        f["raw_data"]["contract"][0]["Permission_id"] = json!("2147483648");
        cases.push(f);
        let mut f = fixture();
        f["signature"] = json!(["00"]);
        cases.push(f);
        let mut f = fixture();
        f["signature"] = json!("00");
        cases.push(f);
        let mut f = fixture();
        f["visible"] = json!("false");
        cases.push(f);
        let mut f = fixture();
        f["raw_data"]["auths"] = json!([{}]);
        cases.push(f);
        for f in cases {
            assert!(from_json(&f).is_err());
        }
    }
    #[test]
    fn signed_json_and_existing_hex_queries_preserve_truthful_signers() {
        // Synthetic test key only; no runtime wallet, signer or node is used.
        let key = [0x42; 32];
        let address = Address::from_uncompressed_pubkey(
            &tron_crypto::signature::public_key_from_private(&key).unwrap(),
        )
        .unwrap();
        let mut f = fixture();
        f["raw_data"]["contract"][0]["parameter"]["value"]["owner_address"] =
            json!(hex::encode(address.as_bytes()));
        let mut tx = from_json(&f).unwrap();
        tron_types::sign_transaction(&mut tx, &key).unwrap();
        let state = state(address.as_bytes());
        let wire = tx.encode_to_vec();
        let signed = extension(&tx, &wire, &tron_types::tx_id(&tx).unwrap(), false).unwrap()
            ["transaction"]
            .clone();
        let json = methods::get_sign_weight(&json!([signed.clone()]), &state).unwrap();
        let hex = methods::get_sign_weight(&json!([format!("0x{}", hex::encode(&wire))]), &state)
            .unwrap();
        assert_eq!(json, hex);
        assert_eq!(json["result"]["code"], "ENOUGH_PERMISSION");
        assert_eq!(json["current_weight"], 1);
        assert_eq!(json["approved_list"].as_array().unwrap().len(), 1);
        assert_eq!(
            from_json(&json["transaction"]["transaction"])
                .unwrap()
                .encode_to_vec(),
            wire
        );
        let approved = methods::get_approved_list(&json!([signed]), &state).unwrap();
        assert_eq!(approved["approved_list"].as_array().unwrap().len(), 1);
        // Memo mutation invalidates that original signature's permission.
        let mut changed = json["transaction"]["transaction"].clone();
        changed["raw_data"]["data"] = json!("01");
        let mutated = methods::get_sign_weight(&json!([changed]), &state).unwrap();
        assert_ne!(mutated["result"]["code"], "ENOUGH_PERMISSION");
        assert_eq!(mutated["current_weight"], 0);
    }
    #[test]
    fn existing_wire_unknown_raw_bytes_keep_original_display_identity() {
        let tx = from_json(&fixture()).unwrap();
        let mut raw = tx.raw_data.as_ref().unwrap().encode_to_vec();
        // Unknown raw_data tag127 varint1, a legacy full-wire input.
        raw.extend_from_slice(&[0xf8, 0x07, 0x01]);
        let mut wire = vec![0x0a];
        let mut n = raw.len();
        while n >= 128 {
            wire.push((n as u8 & 127) | 128);
            n >>= 7;
        }
        wire.push(n as u8);
        wire.extend_from_slice(&raw);
        let parsed = Transaction::decode(wire.as_slice()).unwrap();
        let id = tron_types::tx_id_from_tx_bytes(&wire).unwrap();
        let ext = extension(&parsed, &wire, &id, false).unwrap();
        assert_eq!(ext["transaction"]["raw_data_hex"], hex::encode(&raw));
        assert_eq!(
            ext["transaction"]["txID"],
            hex::encode(tron_crypto::hash::sha256(&raw))
        );
    }
    #[test]
    fn original_wire_prehash_is_used_for_real_signed_unknown_fields() {
        let key = [0x43; 32];
        let address = Address::from_uncompressed_pubkey(
            &tron_crypto::signature::public_key_from_private(&key).unwrap(),
        )
        .unwrap();
        let mut f = fixture();
        f["raw_data"]["contract"][0]["parameter"]["value"]["owner_address"] =
            json!(hex::encode(address.as_bytes()));
        let tx = from_json(&f).unwrap();
        let mut raw = tx.raw_data.as_ref().unwrap().encode_to_vec();
        raw.extend_from_slice(&[0xf8, 0x07, 0x01]);
        let id = tron_crypto::hash::sha256(&raw);
        let signature = tron_crypto::signature::RecoverableSignature::sign_prehash(&key, &id)
            .unwrap()
            .to_bytes();
        let mut wire = vec![0x0a];
        let mut n = raw.len();
        while n >= 128 {
            wire.push((n as u8 & 127) | 128);
            n >>= 7;
        }
        wire.push(n as u8);
        wire.extend_from_slice(&raw);
        wire.extend_from_slice(&[0x12, 65]);
        wire.extend_from_slice(&signature);
        let state = state(address.as_bytes());
        let response =
            methods::get_sign_weight(&json!([format!("0x{}", hex::encode(&wire))]), &state)
                .unwrap();
        assert_eq!(response["result"]["code"], "ENOUGH_PERMISSION");
        assert_eq!(response["current_weight"], 1);
        assert_eq!(
            response["transaction"]["transaction"]["txID"],
            hex::encode(id)
        );
        assert_eq!(
            response["transaction"]["transaction"]["raw_data_hex"],
            hex::encode(raw)
        );
        let approved =
            methods::get_approved_list(&json!([format!("0x{}", hex::encode(&wire))]), &state)
                .unwrap();
        assert_eq!(approved["approved_list"], response["approved_list"]);
        // The unchanged canonical-struct helper has no wire prehash and still
        // evaluates exactly the decoded canonical transaction it was handed.
        let parsed = Transaction::decode(wire.as_slice()).unwrap();
        let canonical = tron_actuator::permission::compute_sign_weight(
            &state.accounts,
            &state.dyn_props,
            &parsed,
        )
        .unwrap();
        assert_eq!(canonical.current_weight, 0);
    }

    #[tokio::test]
    async fn http_permission_routes_accept_unsigned_json_and_visible_addresses() {
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use http_body_util::BodyExt as _;
        use tower::ServiceExt as _;
        let owner_bytes = hex::decode(owner()).unwrap();
        let state = state(&owner_bytes);
        let mut f = fixture();
        f["visible"] = json!(true);
        f["raw_data"]["data"] = json!("6d656d6f");
        let base58 = tron_crypto::base58check::encode_address(&Address::from_raw(
            owner_bytes.try_into().unwrap(),
        ));
        f["raw_data"]["contract"][0]["parameter"]["value"]["owner_address"] = json!(base58);
        for route in ["/wallet/getsignweight", "/wallet/getapprovedlist"] {
            let response = crate::http_rest::router(state.clone())
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(route)
                        .header("Content-Type", "application/json")
                        .body(Body::from(f.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let value: Value = serde_json::from_slice(&body).unwrap();
            assert!(value.get("Error").is_none());
            assert_eq!(
                value["transaction"]["transaction"]["raw_data"]["data"],
                "6d656d6f"
            );
            assert_eq!(
                value["transaction"]["transaction"]["raw_data"]["contract"][0]["parameter"]
                    ["value"]["owner_address"],
                base58
            );
            assert_eq!(value["transaction"]["transaction"]["visible"], true);
        }
    }
}
