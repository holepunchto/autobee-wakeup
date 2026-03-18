/**
 * @typedef {import('autobee')} Autobee
 */

const ProtomuxWakeup = require('protomux-wakeup')
const CoreCoupler = require('core-coupler')

class WakeupHandler {
  constructor(wakeup, discoveryKey) {
    /** @type {AutobeeWakeup} */
    this.wakeup = wakeup

    this.active = true
    this.discoveryKey = discoveryKey
  }

  onpeeractive(peer, session) {
    this.wakeup._bumpWakeupPeer(peer)
  }

  onlookup(req, peer, session) {
    const wakeup = this.wakeup._getWakeupWriters()
    if (wakeup.length === 0) return
    session.announce(peer, wakeup)
  }

  onannounce(wakeup, peer, session) {
    // @todo isForwarding check fit in here?
    this.wakeup.setNeedsWakeupRequest(true)
    this.wakeup.hint(wakeup)
  }
}

module.exports = class AutobeeWakeup extends ReadyResource {
  constructor(opts = {}) {
    /** @type {Autobee} */
    this.auto = opts.auto
    this.owner = !opts.wakeup

    this.protocol = opts.wakeup || new ProtomuxWakeup()
    this.session = null

    this._coupler = null
    this._hints = new Map()

    this._needsWakeupRequest = false
    this._needsWakeup = true
    this._needsWakeupHeads = true
    this._wakeupPeerBound = this._wakeupPeer.bind(this)
  }

  _close() {
    this.session.destroy()
    this.protocol.destroy()
  }

  // @todo need to see how/if these setters fit into autobee flow
  setNeedsWakeupHeads(needs) {
    this._needsWakeupHeads = needs
  }
  setNeedsWakeup(needs) {
    this._needsWakeup = needs
  }
  setNeedsWakeupRequest(needs) {
    this._needsWakeupRequest = needs
  }

  addStream(stream) {
    this.protocol.addStream(stream)
  }

  hint(hints) {
    if (!Array.isArray(hints)) hints = [hints]
    for (const { key, length } of hints) {
      const hex = b4a.toString(key, 'hex')
      const prev = this._hints.get(hex)
      if (!prev || length === -1 || prev < length) this._hints.set(hex, length)
    }

    this.auto.bumpSoon()
  }

  clear() {
    this._hints.clear()
  }

  // @todo needed? used if missed due to forwarding
  broadcastLookup() {
    this.setNeedsWakeupRequest(false)
    this.session.broadcastLookup({})
  }

  setCapability(cap, discoveryKey) {
    if (this.session) this.session.destroy()
    if (!discoveryKey && b4a.equals(cap, this.key)) discoveryKey = this.discoveryKey
    this.session = this.protocol.session(cap, new WakeupHandler(this, discoveryKey || null))

    // incase this session has active peers already, bump the coupler
    for (const peer of this.session.peers) {
      if (peer.active) this._bumpWakeupPeer(peer)
    }
  }

  _bumpWakeupPeer(peer) {
    if (this._coupler) this._coupler.update(peer.stream)
  }

  _wakeupPeer(stream) {
    if (!this.session) return
    const wakeup = this._getWakeupWriters()
    if (wakeup.length === 0) return
    this.session.announceByStream(stream, wakeup)
  }

  _getWakeupWriters() {
    const writers = []

    for (const w of this.auto.writers) {
      if (w.isIndexer || w.pending === null) continue
      writers.push({ key: w.core.key, length: w.length })
    }

    return writers
  }

  recouple() {
    if (this._coupler) this._coupler.destroy()
    const core = this.base.system.bee.core
    this._coupler = new CoreCoupler(core, this._wakeupPeerBound)
  }
}
