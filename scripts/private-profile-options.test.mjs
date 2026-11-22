import test from 'node:test';
import assert from 'node:assert/strict';
import {privateProfileOptions,indexCaptureLines,privateWalletGatewayConfig} from './private-profile-options.mjs';

test('legacy defaults retain the existing private identity and disabled history captures',()=>{
 const options=privateProfileOptions();
 assert.deepEqual(options,{networkId:2026111501,captureHistory:false});
 assert.deepEqual(indexCaptureLines(options.captureHistory),[
  'capture_native = false','capture_trc20 = false','capture_trc721 = false',
  'capture_internal = false','capture_logs = false'
 ]);
});
test('an explicit private ID and capture switch produce a consistent fresh profile policy',()=>{
 const options=privateProfileOptions({'private-network-id':'2026111602','capture-history':'true'});
 assert.equal(options.networkId,2026111602);
 const expected=[
  'capture_native = true','capture_trc20 = true','capture_trc721 = false',
  'capture_internal = false','capture_logs = true'
 ];
 // The generator uses this same pure result for every role before any ledger init.
 for(const role of ['producer','follower-a','follower-b'])
  assert.deepEqual(indexCaptureLines(options.captureHistory),expected,role);
 assert.equal(privateProfileOptions({'capture-history':'false'}).captureHistory,false);
});
test('private network ID validation excludes public, ambiguous and overflowing identities',()=>{
 for(const id of ['1','11111','201910292','999999999','2147483648','02026111602',
  '2026111602.0','2.026111602e9','+2026111602','2026111602 ','',null,2026111602])
  assert.throws(()=>privateProfileOptions({'private-network-id':id}),/private-network-id/);
 for(const id of ['1000000000','2147483647'])
  assert.equal(privateProfileOptions({'private-network-id':id}).networkId,Number(id));
});
test('history capture accepts only explicit CLI boolean words',()=>{
 for(const capture of ['TRUE','False','1','0','yes','',null,true,false])
  assert.throws(()=>privateProfileOptions({'capture-history':capture}),/capture-history/);
 assert.throws(()=>indexCaptureLines('true'),/must be boolean/);
});


test('every fresh private runtime emits an enabled gateway candidate bound to its native marker',()=>{
 const first=privateWalletGatewayConfig('/srv/private-one/network',19100);
 const second=privateWalletGatewayConfig('/srv/private-two/network',20100);
 assert.deepEqual(first,{upstream_port:19120,tronlink_multicall_enabled:true,
  tronlink_multicall_profile_file:'/srv/private-one/network/follower-a/private-profile.json'});
 assert.equal(second.tronlink_multicall_profile_file,'/srv/private-two/network/follower-a/private-profile.json');
 assert.equal(second.upstream_port,20120);
 assert(!JSON.stringify(first).includes('0000000000000000'),'no stale genesis literal');
});
test('private gateway provisioning rejects relative roots and invalid port groups',()=>{
 for(const root of ['relative','',null,42])
  assert.throws(()=>privateWalletGatewayConfig(root,19100),/absolute filesystem path/);
 for(const port of [1023,65500,19100.5,null,'19100'])
  assert.throws(()=>privateWalletGatewayConfig('/srv/private',port),/port base/);
});
