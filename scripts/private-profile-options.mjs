import {isAbsolute,join} from 'node:path';
// Pure CLI options; importing this module never generates keys or touches runtime.
export function privateProfileOptions(opts={}){
 const text=opts['private-network-id']===undefined?'2026111501':opts['private-network-id'];
 if(typeof text!=='string'||!/^[1-9][0-9]{9}$/.test(text))
  throw Error('private-network-id must be a canonical decimal private network ID');
 const networkId=Number(text);
 if(!Number.isSafeInteger(networkId)||networkId<1_000_000_000||networkId>2_147_483_647)
  throw Error('private-network-id must be1000000000..2147483647');
 const capture=opts['capture-history']===undefined?'false':opts['capture-history'];
 if(capture!=='true'&&capture!=='false')
  throw Error('capture-history must be true or false');
 return {networkId,captureHistory:capture==='true'};
}
export function indexCaptureLines(captureHistory){
 if(typeof captureHistory!=='boolean')throw Error('capture history must be boolean');
 return ['capture_native = '+captureHistory,'capture_trc20 = '+captureHistory,
  'capture_trc721 = false','capture_internal = false','capture_logs = '+captureHistory];
}


// This candidate follows the native marker across fresh ledgers; no genesis is copied.
export function privateWalletGatewayConfig(root,portBase){
 if(typeof root!=='string'||!isAbsolute(root)||root.includes('\0'))
  throw Error('private gateway root must be an absolute filesystem path');
 if(!Number.isInteger(portBase)||portBase<1024||portBase+45>65535)
  throw Error('invalid private gateway port base');
 return {upstream_port:portBase+20,tronlink_multicall_enabled:true,
  tronlink_multicall_profile_file:join(root,'follower-a','private-profile.json')};
}
