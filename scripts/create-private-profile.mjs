// Persistent private profile generator. It never starts, stops or installs services.
// Requires Node24, workspace TronWeb and an already-built isolated private-node example.
import {TronWeb} from 'tronweb';
import {mkdir,readFile,writeFile,access} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {privateProfileOptions,indexCaptureLines,privateWalletGatewayConfig} from './private-profile-options.mjs';
const argv=process.argv.slice(2),opts={};
for(let i=0;i<argv.length;i+=2){if(!argv[i].startsWith('--')||argv[i+1]===undefined)throw Error('expected --name value');opts[argv[i].slice(2)]=argv[i+1];}
if(!opts.root||!opts.capture||!opts.binary)throw Error('usage: node create-private-profile.mjs --root ABSOLUTE_NEW_RUNTIME --capture MAINNET_JSON --binary PRIVATE_NODE [--port-base 19100] [--private-network-id 2026111602] [--capture-history true]');
if(typeof JSON.rawJSON!=='function')throw Error('Node24 JSON.rawJSON required for exact SUN integers');
const exact=(v)=>JSON.stringify(v,(_k,x)=>typeof x==='bigint'?JSON.rawJSON(x.toString()):x,2)+'\n';
const root=resolve(opts.root),binary=resolve(opts.binary),capture=JSON.parse(await readFile(resolve(opts.capture),'utf8'));
const {networkId,captureHistory}=privateProfileOptions(opts);
const base=Number(opts['port-base']??19100);
const tipPoll=Number(opts['tip-poll-ms']??1000);
if(!Number.isInteger(tipPoll)||tipPoll<250||tipPoll>5000)throw Error('tip-poll-ms must be250..5000');
if(!Number.isInteger(base)||base<1024||base+45>65535)throw Error('invalid port-base');
await mkdir(root,{recursive:true,mode:0o700});
for(const name of ['profile.json','accounts.json','wallet-gateway.json','producer','follower-a','follower-b']){
 try{await access(join(root,name));throw Error('Refusing to replace existing private runtime: '+name);}catch(e){if(e.code!=='ENOENT')throw e;}
}
const mapping={"getMaintenanceTimeInterval":"MAINTENANCE_TIME_INTERVAL","getAccountUpgradeCost":"ACCOUNT_UPGRADE_COST","getCreateAccountFee":"CREATE_ACCOUNT_FEE","getTransactionFee":"TRANSACTION_FEE","getAssetIssueFee":"ASSET_ISSUE_FEE","getWitnessPayPerBlock":"WITNESS_PAY_PER_BLOCK","getWitnessStandbyAllowance":"WITNESS_STANDBY_ALLOWANCE","getCreateNewAccountFeeInSystemContract":"CREATE_NEW_ACCOUNT_FEE_IN_SYSTEM_CONTRACT","getCreateNewAccountBandwidthRate":"CREATE_NEW_ACCOUNT_BANDWIDTH_RATE","getAllowCreationOfContracts":"ALLOW_CREATION_OF_CONTRACTS","getRemoveThePowerOfTheGr":"REMOVE_THE_POWER_OF_THE_GR","getEnergyFee":"ENERGY_FEE","getExchangeCreateFee":"EXCHANGE_CREATE_FEE","getMaxCpuTimeOfOneTx":"MAX_CPU_TIME_OF_ONE_TX","getAllowUpdateAccountName":"ALLOW_UPDATE_ACCOUNT_NAME","getAllowSameTokenName":" ALLOW_SAME_TOKEN_NAME","getAllowDelegateResource":"ALLOW_DELEGATE_RESOURCE","getTotalEnergyLimit":"TOTAL_ENERGY_LIMIT","getAllowTvmTransferTrc10":"ALLOW_TVM_TRANSFER_TRC10","getTotalEnergyCurrentLimit":"TOTAL_ENERGY_CURRENT_LIMIT","getAllowMultiSign":"ALLOW_MULTI_SIGN","getAllowAdaptiveEnergy":"ALLOW_ADAPTIVE_ENERGY","getTotalEnergyTargetLimit":"TOTAL_ENERGY_TARGET_LIMIT","getTotalEnergyAverageUsage":"TOTAL_ENERGY_AVERAGE_USAGE","getUpdateAccountPermissionFee":"UPDATE_ACCOUNT_PERMISSION_FEE","getMultiSignFee":"MULTI_SIGN_FEE","getAllowAccountStateRoot":"ALLOW_ACCOUNT_STATE_ROOT","getAllowProtoFilterNum":"ALLOW_PROTO_FILTER_NUM","getAllowTvmConstantinople":"ALLOW_TVM_CONSTANTINOPLE","getAllowTvmSolidity059":"ALLOW_TVM_SOLIDITY_059","getAllowTvmIstanbul":"ALLOW_TVM_ISTANBUL","getAllowShieldedTRC20Transaction":"ALLOW_SHIELDED_TRC20_TRANSACTION","getForbidTransferToContract":"FORBID_TRANSFER_TO_CONTRACT","getAdaptiveResourceLimitTargetRatio":"ADAPTIVE_RESOURCE_LIMIT_TARGET_RATIO","getAdaptiveResourceLimitMultiplier":"ADAPTIVE_RESOURCE_LIMIT_MULTIPLIER","getChangeDelegation":"CHANGE_DELEGATION","getWitness127PayPerBlock":"WITNESS_127_PAY_PER_BLOCK","getAllowMarketTransaction":"ALLOW_MARKET_TRANSACTION","getMarketSellFee":"MARKET_SELL_FEE","getMarketCancelFee":"MARKET_CANCEL_FEE","getAllowPBFT":"ALLOW_PBFT","getAllowTransactionFeePool":"ALLOW_TRANSACTION_FEE_POOL","getMaxFeeLimit":"MAX_FEE_LIMIT","getAllowOptimizeBlackHole":"ALLOW_BLACKHOLE_OPTIMIZATION","getAllowNewResourceModel":"ALLOW_NEW_RESOURCE_MODEL","getAllowTvmFreeze":"ALLOW_TVM_FREEZE","getAllowTvmVote":"ALLOW_TVM_VOTE","getAllowTvmLondon":"ALLOW_TVM_LONDON","getAllowTvmCompatibleEvm":"ALLOW_TVM_COMPATIBLE_EVM","getAllowAccountAssetOptimization":"ALLOW_ACCOUNT_ASSET_OPTIMIZATION","getFreeNetLimit":"FREE_NET_LIMIT","getTotalNetLimit":"TOTAL_NET_LIMIT","getAllowHigherLimitForMaxCpuTimeOfOneTx":"ALLOW_HIGHER_LIMIT_FOR_MAX_CPU_TIME_OF_ONE_TX","getAllowAssetOptimization":"ALLOW_ASSET_OPTIMIZATION","getAllowNewReward":"ALLOW_NEW_REWARD","getMemoFee":"MEMO_FEE","getAllowDelegateOptimization":"ALLOW_DELEGATE_OPTIMIZATION","getUnfreezeDelayDays":"UNFREEZE_DELAY_DAYS","getAllowOptimizedReturnValueOfChainId":"ALLOW_OPTIMIZED_RETURN_VALUE_OF_CHAIN_ID","getAllowDynamicEnergy":"ALLOW_DYNAMIC_ENERGY","getDynamicEnergyThreshold":"DYNAMIC_ENERGY_THRESHOLD","getDynamicEnergyIncreaseFactor":"DYNAMIC_ENERGY_INCREASE_FACTOR","getDynamicEnergyMaxFactor":"DYNAMIC_ENERGY_MAX_FACTOR","getAllowTvmShangHai":"ALLOW_TVM_SHANGHAI","getAllowCancelAllUnfreezeV2":"ALLOW_CANCEL_ALL_UNFREEZE_V2","getMaxDelegateLockPeriod":"MAX_DELEGATE_LOCK_PERIOD","getAllowOldRewardOpt":"ALLOW_OLD_REWARD_OPT","getAllowEnergyAdjustment":"ALLOW_ENERGY_ADJUSTMENT","getMaxCreateAccountTxSize":"MAX_CREATE_ACCOUNT_TX_SIZE","getAllowStrictMath":"ALLOW_STRICT_MATH","getConsensusLogicOptimization":"CONSENSUS_LOGIC_OPTIMIZATION","getAllowTvmCancun":"ALLOW_TVM_CANCUN","getAllowTvmBlob":"ALLOW_TVM_BLOB","getAllowTvmSelfdestructRestriction":"ALLOW_TVM_SELFDESTRUCT_RESTRICTION","getProposalExpireTime":"PROPOSAL_EXPIRE_TIME","getAllowTvmOsaka":"ALLOW_TVM_OSAKA","getAllowTvmPrague":"ALLOW_TVM_PRAGUE","getAllowHardenResourceCalculation":"ALLOW_HARDEN_RESOURCE_CALCULATION","getAllowHardenExchangeCalculation":"ALLOW_HARDEN_EXCHANGE_CALCULATION","getAllowOptimizeTvmStorage":"ALLOW_OPTIMIZE_TVM_STORAGE","getAllowOptimizeTvmStorageKey":"ALLOW_OPTIMIZE_TVM_STORAGE_KEY"};
// ALLOW_SAME_TOKEN_NAME intentionally has Java's canonical leading-space stored key.
const parameters={};
for(const entry of capture.getchainparameters?.chainParameter??[]){
 const value=entry.value??0;
 if(!Number.isSafeInteger(value))throw Error('Captured parameter is not an exact integer: '+entry.key);
 if(mapping[entry.key])parameters[mapping[entry.key]]=value;
 else if(value!==0)throw Error('Unrecognized active mainnet parameter: '+entry.key);
}
if(Object.keys(parameters).length<60)throw Error('Incomplete mainnet chain parameter capture');
parameters.ADAPTIVE_RESOURCE_LIMIT_TARGET_RATIO*=24*60;
const resource=capture.getaccountresource??{};
for(const [key,field] of [['TOTAL_ENERGY_WEIGHT','TotalEnergyWeight'],['TOTAL_NET_WEIGHT','TotalNetWeight'],['TOTAL_NET_LIMIT','TotalNetLimit']]){
 const value=resource[field];
 if(!Number.isSafeInteger(value)||value<=0)throw Error('Missing exact mainnet resource '+field);
 parameters[key]=value;
}
if(resource.TotalEnergyLimit!==parameters.TOTAL_ENERGY_CURRENT_LIMIT)throw Error('Captured EnergyLimit disagrees with chain parameter');
if(parameters.ALLOW_PBFT!==0)throw Error('Private keyring currently models DPoS, not multi-key PBFT');
const accounts={};
for(const name of [...Array.from({length:27},(_,i)=>'sr'+String(i).padStart(2,'0')),'treasury','resource-sponsor','admin','operator','alice','bob','keeper']){
 const account=await TronWeb.createAccount();
 accounts[name]={address:account.address.base58,hex:account.address.hex.toLowerCase(),key:account.privateKey};
}
const stakes=[{address:accounts['resource-sponsor'].hex,
 energy_sun:BigInt(parameters.TOTAL_ENERGY_WEIGHT)*1000000n,
 bandwidth_sun:BigInt(parameters.TOTAL_NET_WEIGHT)*1000000n}];
const assets=Object.entries(accounts).map(([name,a])=>({name,address:a.hex,
 balance:name==='resource-sponsor'?stakes[0].energy_sun+stakes[0].bandwidth_sun+1000000000000n:
 name==='treasury'?1000000000000000n:name.startsWith('sr')?1000000000n:1000000000000n}));
const head=capture.getnowblock??{},headRaw=head.block_header?.raw_data;
if(headRaw?.version!==39)throw Error('Captured protocol version must match supported39; validate a newer protocol before using it');
const profile={schema:1,name:'private-mainnet-like-v1',chain_id:networkId,p2p_network_id:networkId,
 timestamp_ms:Math.floor(Date.now()/3000)*3000-6000,native_unfreeze_delay_secs:300,
 source:{network:'mainnet',source:capture.source,observed_at:capture.observed_at,
  genesis:capture.getblockbynum?.blockID,head:head.blockID,height:headRaw.number,version:headRaw.version},
 witnesses:Array.from({length:27},(_,i)=>({address:accounts['sr'+String(i).padStart(2,'0')].hex,vote_count:100000000-i})),
 assets,stakes,parameters};
await writeFile(join(root,'profile.json'),exact(profile),{mode:0o600,flag:'wx'});
const identity=JSON.parse(execFileSync(binary,['describe',join(root,'profile.json')],{encoding:'utf8'}));
await writeFile(join(root,'identity.json'),exact(identity),{mode:0o600,flag:'wx'});
await writeFile(join(root,'accounts.json'),exact(accounts),{mode:0o600,flag:'wx'});
await writeFile(join(root,'witness-keys.json'),exact(Array.from({length:27},(_,i)=>accounts['sr'+String(i).padStart(2,'0')].key)),{mode:0o600,flag:'wx'});
const roles=['producer','follower-a','follower-b'],ports=Object.fromEntries(roles.map((r,i)=>[r,base+i*20]));
for(const role of roles){
 const n=ports[role],producer=role==='producer';
 const peers=(producer?['follower-a','follower-b']:['producer',role==='follower-a'?'follower-b':'follower-a']).map(r=>'127.0.0.1:'+(ports[r]+4));
 const lines=[
  'data_dir = '+JSON.stringify(join(root,role)),
  '[private_network]','manifest = '+JSON.stringify(join(root,'profile.json')),
  'tip_poll_interval_ms = '+tipPoll,
  ...(producer?['witness_keys = '+JSON.stringify(join(root,'witness-keys.json'))]:[]),
  '[storage]','write_buffer_size_mb = 2','max_open_files = 32','block_cache_mb = 64','snapshot_reorg = false',
  '[p2p]','peers = '+JSON.stringify(peers),'discover_enable = false','discover_tree_urls = []','node_discovery_persist = false',
  'listen = true','listen_host = "127.0.0.1"','advertise_port = '+(n+4),'max_peers = 4','min_peers = 1','multi_peer_fetch = false','progress_log_interval = 1',
  '[rpc]','host = "127.0.0.1"','port = '+(n+1),'chain_id = '+profile.chain_id,
  '[http]','host = "127.0.0.1"','port = '+n,
  '[grpc]','host = "127.0.0.1"','port = '+(n+2),
  '[metrics]','host = "127.0.0.1"','port = '+(n+3),
  '[vm]','support_constant = true','save_internal_tx = true','save_featured_internal_tx = true','estimate_energy = true','parallel_exec = false','pipelined_apply = false',
  '[index]','enable = true','capture_state_deltas = true',...indexCaptureLines(captureHistory),
  '[index.archive]','enabled = true','mode = "rolling"','retain_blocks = 100000',
  '[index.firehose]','enable = true','retain_mb = 1024',
  '[index.firehose.history]','enable = true','retain_blocks = 65536','undo_retain_blocks = 10000','batch_blocks = 128','interval_seconds = 3600','ack_port = '+(n+5),
  ...(producer?['[witness]','max_txs_per_block = 1000']:[])
 ];
 await writeFile(join(root,role+'.toml'),lines.join('\n')+'\n',{mode:0o600,flag:'wx'});
}
await writeFile(join(root,'wallet-gateway.json'),exact(privateWalletGatewayConfig(root,base)),{mode:0o600,flag:'wx'});
console.log(exact({root,identity,roles:roles.map(role=>({role,http:ports[role],rpc:ports[role]+1,grpc:ports[role]+2,p2p:ports[role]+4,ack:ports[role]+5})),
 wallet_gateway_config:join(root,'wallet-gateway.json'),
 next:'Configure follower mTLS cluster pins/bootstrap_genesis before starting. Initialize producer and follower-a only; follower-b may bootstrap empty.'}));
