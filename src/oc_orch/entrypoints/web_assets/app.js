// node_modules/@lit/reactive-element/css-tag.js
var t = globalThis;
var e = t.ShadowRoot && (void 0 === t.ShadyCSS || t.ShadyCSS.nativeShadow) && "adoptedStyleSheets" in Document.prototype && "replace" in CSSStyleSheet.prototype;
var s = /* @__PURE__ */ Symbol();
var o = /* @__PURE__ */ new WeakMap();
var n = class {
  constructor(t5, e5, o5) {
    if (this._$cssResult$ = true, o5 !== s) throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");
    this.cssText = t5, this.t = e5;
  }
  get styleSheet() {
    let t5 = this.o;
    const s5 = this.t;
    if (e && void 0 === t5) {
      const e5 = void 0 !== s5 && 1 === s5.length;
      e5 && (t5 = o.get(s5)), void 0 === t5 && ((this.o = t5 = new CSSStyleSheet()).replaceSync(this.cssText), e5 && o.set(s5, t5));
    }
    return t5;
  }
  toString() {
    return this.cssText;
  }
};
var r = (t5) => new n("string" == typeof t5 ? t5 : t5 + "", void 0, s);
var S = (s5, o5) => {
  if (e) s5.adoptedStyleSheets = o5.map((t5) => t5 instanceof CSSStyleSheet ? t5 : t5.styleSheet);
  else for (const e5 of o5) {
    const o6 = document.createElement("style"), n4 = t.litNonce;
    void 0 !== n4 && o6.setAttribute("nonce", n4), o6.textContent = e5.cssText, s5.appendChild(o6);
  }
};
var c = e ? (t5) => t5 : (t5) => t5 instanceof CSSStyleSheet ? ((t6) => {
  let e5 = "";
  for (const s5 of t6.cssRules) e5 += s5.cssText;
  return r(e5);
})(t5) : t5;

// node_modules/@lit/reactive-element/reactive-element.js
var { is: i2, defineProperty: e2, getOwnPropertyDescriptor: h, getOwnPropertyNames: r2, getOwnPropertySymbols: o2, getPrototypeOf: n2 } = Object;
var a = globalThis;
var c2 = a.trustedTypes;
var l = c2 ? c2.emptyScript : "";
var p = a.reactiveElementPolyfillSupport;
var d = (t5, s5) => t5;
var u = { toAttribute(t5, s5) {
  switch (s5) {
    case Boolean:
      t5 = t5 ? l : null;
      break;
    case Object:
    case Array:
      t5 = null == t5 ? t5 : JSON.stringify(t5);
  }
  return t5;
}, fromAttribute(t5, s5) {
  let i8 = t5;
  switch (s5) {
    case Boolean:
      i8 = null !== t5;
      break;
    case Number:
      i8 = null === t5 ? null : Number(t5);
      break;
    case Object:
    case Array:
      try {
        i8 = JSON.parse(t5);
      } catch (t6) {
        i8 = null;
      }
  }
  return i8;
} };
var f = (t5, s5) => !i2(t5, s5);
var b = { attribute: true, type: String, converter: u, reflect: false, useDefault: false, hasChanged: f };
Symbol.metadata ??= /* @__PURE__ */ Symbol("metadata"), a.litPropertyMetadata ??= /* @__PURE__ */ new WeakMap();
var y = class extends HTMLElement {
  static addInitializer(t5) {
    this._$Ei(), (this.l ??= []).push(t5);
  }
  static get observedAttributes() {
    return this.finalize(), this._$Eh && [...this._$Eh.keys()];
  }
  static createProperty(t5, s5 = b) {
    if (s5.state && (s5.attribute = false), this._$Ei(), this.prototype.hasOwnProperty(t5) && ((s5 = Object.create(s5)).wrapped = true), this.elementProperties.set(t5, s5), !s5.noAccessor) {
      const i8 = /* @__PURE__ */ Symbol(), h5 = this.getPropertyDescriptor(t5, i8, s5);
      void 0 !== h5 && e2(this.prototype, t5, h5);
    }
  }
  static getPropertyDescriptor(t5, s5, i8) {
    const { get: e5, set: r4 } = h(this.prototype, t5) ?? { get() {
      return this[s5];
    }, set(t6) {
      this[s5] = t6;
    } };
    return { get: e5, set(s6) {
      const h5 = e5?.call(this);
      r4?.call(this, s6), this.requestUpdate(t5, h5, i8);
    }, configurable: true, enumerable: true };
  }
  static getPropertyOptions(t5) {
    return this.elementProperties.get(t5) ?? b;
  }
  static _$Ei() {
    if (this.hasOwnProperty(d("elementProperties"))) return;
    const t5 = n2(this);
    t5.finalize(), void 0 !== t5.l && (this.l = [...t5.l]), this.elementProperties = new Map(t5.elementProperties);
  }
  static finalize() {
    if (this.hasOwnProperty(d("finalized"))) return;
    if (this.finalized = true, this._$Ei(), this.hasOwnProperty(d("properties"))) {
      const t6 = this.properties, s5 = [...r2(t6), ...o2(t6)];
      for (const i8 of s5) this.createProperty(i8, t6[i8]);
    }
    const t5 = this[Symbol.metadata];
    if (null !== t5) {
      const s5 = litPropertyMetadata.get(t5);
      if (void 0 !== s5) for (const [t6, i8] of s5) this.elementProperties.set(t6, i8);
    }
    this._$Eh = /* @__PURE__ */ new Map();
    for (const [t6, s5] of this.elementProperties) {
      const i8 = this._$Eu(t6, s5);
      void 0 !== i8 && this._$Eh.set(i8, t6);
    }
    this.elementStyles = this.finalizeStyles(this.styles);
  }
  static finalizeStyles(s5) {
    const i8 = [];
    if (Array.isArray(s5)) {
      const e5 = new Set(s5.flat(1 / 0).reverse());
      for (const s6 of e5) i8.unshift(c(s6));
    } else void 0 !== s5 && i8.push(c(s5));
    return i8;
  }
  static _$Eu(t5, s5) {
    const i8 = s5.attribute;
    return false === i8 ? void 0 : "string" == typeof i8 ? i8 : "string" == typeof t5 ? t5.toLowerCase() : void 0;
  }
  constructor() {
    super(), this._$Ep = void 0, this.isUpdatePending = false, this.hasUpdated = false, this._$Em = null, this._$Ev();
  }
  _$Ev() {
    this._$ES = new Promise((t5) => this.enableUpdating = t5), this._$AL = /* @__PURE__ */ new Map(), this._$E_(), this.requestUpdate(), this.constructor.l?.forEach((t5) => t5(this));
  }
  addController(t5) {
    (this._$EO ??= /* @__PURE__ */ new Set()).add(t5), void 0 !== this.renderRoot && this.isConnected && t5.hostConnected?.();
  }
  removeController(t5) {
    this._$EO?.delete(t5);
  }
  _$E_() {
    const t5 = /* @__PURE__ */ new Map(), s5 = this.constructor.elementProperties;
    for (const i8 of s5.keys()) this.hasOwnProperty(i8) && (t5.set(i8, this[i8]), delete this[i8]);
    t5.size > 0 && (this._$Ep = t5);
  }
  createRenderRoot() {
    const t5 = this.shadowRoot ?? this.attachShadow(this.constructor.shadowRootOptions);
    return S(t5, this.constructor.elementStyles), t5;
  }
  connectedCallback() {
    this.renderRoot ??= this.createRenderRoot(), this.enableUpdating(true), this._$EO?.forEach((t5) => t5.hostConnected?.());
  }
  enableUpdating(t5) {
  }
  disconnectedCallback() {
    this._$EO?.forEach((t5) => t5.hostDisconnected?.());
  }
  attributeChangedCallback(t5, s5, i8) {
    this._$AK(t5, i8);
  }
  _$ET(t5, s5) {
    const i8 = this.constructor.elementProperties.get(t5), e5 = this.constructor._$Eu(t5, i8);
    if (void 0 !== e5 && true === i8.reflect) {
      const h5 = (void 0 !== i8.converter?.toAttribute ? i8.converter : u).toAttribute(s5, i8.type);
      this._$Em = t5, null == h5 ? this.removeAttribute(e5) : this.setAttribute(e5, h5), this._$Em = null;
    }
  }
  _$AK(t5, s5) {
    const i8 = this.constructor, e5 = i8._$Eh.get(t5);
    if (void 0 !== e5 && this._$Em !== e5) {
      const t6 = i8.getPropertyOptions(e5), h5 = "function" == typeof t6.converter ? { fromAttribute: t6.converter } : void 0 !== t6.converter?.fromAttribute ? t6.converter : u;
      this._$Em = e5;
      const r4 = h5.fromAttribute(s5, t6.type);
      this[e5] = r4 ?? this._$Ej?.get(e5) ?? r4, this._$Em = null;
    }
  }
  requestUpdate(t5, s5, i8, e5 = false, h5) {
    if (void 0 !== t5) {
      const r4 = this.constructor;
      if (false === e5 && (h5 = this[t5]), i8 ??= r4.getPropertyOptions(t5), !((i8.hasChanged ?? f)(h5, s5) || i8.useDefault && i8.reflect && h5 === this._$Ej?.get(t5) && !this.hasAttribute(r4._$Eu(t5, i8)))) return;
      this.C(t5, s5, i8);
    }
    false === this.isUpdatePending && (this._$ES = this._$EP());
  }
  C(t5, s5, { useDefault: i8, reflect: e5, wrapped: h5 }, r4) {
    i8 && !(this._$Ej ??= /* @__PURE__ */ new Map()).has(t5) && (this._$Ej.set(t5, r4 ?? s5 ?? this[t5]), true !== h5 || void 0 !== r4) || (this._$AL.has(t5) || (this.hasUpdated || i8 || (s5 = void 0), this._$AL.set(t5, s5)), true === e5 && this._$Em !== t5 && (this._$Eq ??= /* @__PURE__ */ new Set()).add(t5));
  }
  async _$EP() {
    this.isUpdatePending = true;
    try {
      await this._$ES;
    } catch (t6) {
      Promise.reject(t6);
    }
    const t5 = this.scheduleUpdate();
    return null != t5 && await t5, !this.isUpdatePending;
  }
  scheduleUpdate() {
    return this.performUpdate();
  }
  performUpdate() {
    if (!this.isUpdatePending) return;
    if (!this.hasUpdated) {
      if (this.renderRoot ??= this.createRenderRoot(), this._$Ep) {
        for (const [t7, s6] of this._$Ep) this[t7] = s6;
        this._$Ep = void 0;
      }
      const t6 = this.constructor.elementProperties;
      if (t6.size > 0) for (const [s6, i8] of t6) {
        const { wrapped: t7 } = i8, e5 = this[s6];
        true !== t7 || this._$AL.has(s6) || void 0 === e5 || this.C(s6, void 0, i8, e5);
      }
    }
    let t5 = false;
    const s5 = this._$AL;
    try {
      t5 = this.shouldUpdate(s5), t5 ? (this.willUpdate(s5), this._$EO?.forEach((t6) => t6.hostUpdate?.()), this.update(s5)) : this._$EM();
    } catch (s6) {
      throw t5 = false, this._$EM(), s6;
    }
    t5 && this._$AE(s5);
  }
  willUpdate(t5) {
  }
  _$AE(t5) {
    this._$EO?.forEach((t6) => t6.hostUpdated?.()), this.hasUpdated || (this.hasUpdated = true, this.firstUpdated(t5)), this.updated(t5);
  }
  _$EM() {
    this._$AL = /* @__PURE__ */ new Map(), this.isUpdatePending = false;
  }
  get updateComplete() {
    return this.getUpdateComplete();
  }
  getUpdateComplete() {
    return this._$ES;
  }
  shouldUpdate(t5) {
    return true;
  }
  update(t5) {
    this._$Eq &&= this._$Eq.forEach((t6) => this._$ET(t6, this[t6])), this._$EM();
  }
  updated(t5) {
  }
  firstUpdated(t5) {
  }
};
y.elementStyles = [], y.shadowRootOptions = { mode: "open" }, y[d("elementProperties")] = /* @__PURE__ */ new Map(), y[d("finalized")] = /* @__PURE__ */ new Map(), p?.({ ReactiveElement: y }), (a.reactiveElementVersions ??= []).push("2.1.2");

// node_modules/lit-html/lit-html.js
var t2 = globalThis;
var i3 = (t5) => t5;
var s2 = t2.trustedTypes;
var e3 = s2 ? s2.createPolicy("lit-html", { createHTML: (t5) => t5 }) : void 0;
var h2 = "$lit$";
var o3 = `lit$${Math.random().toFixed(9).slice(2)}$`;
var n3 = "?" + o3;
var r3 = `<${n3}>`;
var l2 = document;
var c3 = () => l2.createComment("");
var a2 = (t5) => null === t5 || "object" != typeof t5 && "function" != typeof t5;
var u2 = Array.isArray;
var d2 = (t5) => u2(t5) || "function" == typeof t5?.[Symbol.iterator];
var f2 = "[ 	\n\f\r]";
var v = /<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g;
var _ = /-->/g;
var m = />/g;
var p2 = RegExp(`>|${f2}(?:([^\\s"'>=/]+)(${f2}*=${f2}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`, "g");
var g = /'/g;
var $ = /"/g;
var y2 = /^(?:script|style|textarea|title)$/i;
var x = (t5) => (i8, ...s5) => ({ _$litType$: t5, strings: i8, values: s5 });
var b2 = x(1);
var w = x(2);
var T = x(3);
var E = /* @__PURE__ */ Symbol.for("lit-noChange");
var A = /* @__PURE__ */ Symbol.for("lit-nothing");
var C = /* @__PURE__ */ new WeakMap();
var P = l2.createTreeWalker(l2, 129);
function V(t5, i8) {
  if (!u2(t5) || !t5.hasOwnProperty("raw")) throw Error("invalid template strings array");
  return void 0 !== e3 ? e3.createHTML(i8) : i8;
}
var N = (t5, i8) => {
  const s5 = t5.length - 1, e5 = [];
  let n4, l4 = 2 === i8 ? "<svg>" : 3 === i8 ? "<math>" : "", c5 = v;
  for (let i9 = 0; i9 < s5; i9++) {
    const s6 = t5[i9];
    let a3, u5, d5 = -1, f3 = 0;
    for (; f3 < s6.length && (c5.lastIndex = f3, u5 = c5.exec(s6), null !== u5); ) f3 = c5.lastIndex, c5 === v ? "!--" === u5[1] ? c5 = _ : void 0 !== u5[1] ? c5 = m : void 0 !== u5[2] ? (y2.test(u5[2]) && (n4 = RegExp("</" + u5[2], "g")), c5 = p2) : void 0 !== u5[3] && (c5 = p2) : c5 === p2 ? ">" === u5[0] ? (c5 = n4 ?? v, d5 = -1) : void 0 === u5[1] ? d5 = -2 : (d5 = c5.lastIndex - u5[2].length, a3 = u5[1], c5 = void 0 === u5[3] ? p2 : '"' === u5[3] ? $ : g) : c5 === $ || c5 === g ? c5 = p2 : c5 === _ || c5 === m ? c5 = v : (c5 = p2, n4 = void 0);
    const x2 = c5 === p2 && t5[i9 + 1].startsWith("/>") ? " " : "";
    l4 += c5 === v ? s6 + r3 : d5 >= 0 ? (e5.push(a3), s6.slice(0, d5) + h2 + s6.slice(d5) + o3 + x2) : s6 + o3 + (-2 === d5 ? i9 : x2);
  }
  return [V(t5, l4 + (t5[s5] || "<?>") + (2 === i8 ? "</svg>" : 3 === i8 ? "</math>" : "")), e5];
};
var S2 = class _S {
  constructor({ strings: t5, _$litType$: i8 }, e5) {
    let r4;
    this.parts = [];
    let l4 = 0, a3 = 0;
    const u5 = t5.length - 1, d5 = this.parts, [f3, v3] = N(t5, i8);
    if (this.el = _S.createElement(f3, e5), P.currentNode = this.el.content, 2 === i8 || 3 === i8) {
      const t6 = this.el.content.firstChild;
      t6.replaceWith(...t6.childNodes);
    }
    for (; null !== (r4 = P.nextNode()) && d5.length < u5; ) {
      if (1 === r4.nodeType) {
        if (r4.hasAttributes()) for (const t6 of r4.getAttributeNames()) if (t6.endsWith(h2)) {
          const i9 = v3[a3++], s5 = r4.getAttribute(t6).split(o3), e6 = /([.?@])?(.*)/.exec(i9);
          d5.push({ type: 1, index: l4, name: e6[2], strings: s5, ctor: "." === e6[1] ? I : "?" === e6[1] ? L : "@" === e6[1] ? z : H }), r4.removeAttribute(t6);
        } else t6.startsWith(o3) && (d5.push({ type: 6, index: l4 }), r4.removeAttribute(t6));
        if (y2.test(r4.tagName)) {
          const t6 = r4.textContent.split(o3), i9 = t6.length - 1;
          if (i9 > 0) {
            r4.textContent = s2 ? s2.emptyScript : "";
            for (let s5 = 0; s5 < i9; s5++) r4.append(t6[s5], c3()), P.nextNode(), d5.push({ type: 2, index: ++l4 });
            r4.append(t6[i9], c3());
          }
        }
      } else if (8 === r4.nodeType) if (r4.data === n3) d5.push({ type: 2, index: l4 });
      else {
        let t6 = -1;
        for (; -1 !== (t6 = r4.data.indexOf(o3, t6 + 1)); ) d5.push({ type: 7, index: l4 }), t6 += o3.length - 1;
      }
      l4++;
    }
  }
  static createElement(t5, i8) {
    const s5 = l2.createElement("template");
    return s5.innerHTML = t5, s5;
  }
};
function M(t5, i8, s5 = t5, e5) {
  if (i8 === E) return i8;
  let h5 = void 0 !== e5 ? s5._$Co?.[e5] : s5._$Cl;
  const o5 = a2(i8) ? void 0 : i8._$litDirective$;
  return h5?.constructor !== o5 && (h5?._$AO?.(false), void 0 === o5 ? h5 = void 0 : (h5 = new o5(t5), h5._$AT(t5, s5, e5)), void 0 !== e5 ? (s5._$Co ??= [])[e5] = h5 : s5._$Cl = h5), void 0 !== h5 && (i8 = M(t5, h5._$AS(t5, i8.values), h5, e5)), i8;
}
var R = class {
  constructor(t5, i8) {
    this._$AV = [], this._$AN = void 0, this._$AD = t5, this._$AM = i8;
  }
  get parentNode() {
    return this._$AM.parentNode;
  }
  get _$AU() {
    return this._$AM._$AU;
  }
  u(t5) {
    const { el: { content: i8 }, parts: s5 } = this._$AD, e5 = (t5?.creationScope ?? l2).importNode(i8, true);
    P.currentNode = e5;
    let h5 = P.nextNode(), o5 = 0, n4 = 0, r4 = s5[0];
    for (; void 0 !== r4; ) {
      if (o5 === r4.index) {
        let i9;
        2 === r4.type ? i9 = new k(h5, h5.nextSibling, this, t5) : 1 === r4.type ? i9 = new r4.ctor(h5, r4.name, r4.strings, this, t5) : 6 === r4.type && (i9 = new Z(h5, this, t5)), this._$AV.push(i9), r4 = s5[++n4];
      }
      o5 !== r4?.index && (h5 = P.nextNode(), o5++);
    }
    return P.currentNode = l2, e5;
  }
  p(t5) {
    let i8 = 0;
    for (const s5 of this._$AV) void 0 !== s5 && (void 0 !== s5.strings ? (s5._$AI(t5, s5, i8), i8 += s5.strings.length - 2) : s5._$AI(t5[i8])), i8++;
  }
};
var k = class _k {
  get _$AU() {
    return this._$AM?._$AU ?? this._$Cv;
  }
  constructor(t5, i8, s5, e5) {
    this.type = 2, this._$AH = A, this._$AN = void 0, this._$AA = t5, this._$AB = i8, this._$AM = s5, this.options = e5, this._$Cv = e5?.isConnected ?? true;
  }
  get parentNode() {
    let t5 = this._$AA.parentNode;
    const i8 = this._$AM;
    return void 0 !== i8 && 11 === t5?.nodeType && (t5 = i8.parentNode), t5;
  }
  get startNode() {
    return this._$AA;
  }
  get endNode() {
    return this._$AB;
  }
  _$AI(t5, i8 = this) {
    t5 = M(this, t5, i8), a2(t5) ? t5 === A || null == t5 || "" === t5 ? (this._$AH !== A && this._$AR(), this._$AH = A) : t5 !== this._$AH && t5 !== E && this._(t5) : void 0 !== t5._$litType$ ? this.$(t5) : void 0 !== t5.nodeType ? this.T(t5) : d2(t5) ? this.k(t5) : this._(t5);
  }
  O(t5) {
    return this._$AA.parentNode.insertBefore(t5, this._$AB);
  }
  T(t5) {
    this._$AH !== t5 && (this._$AR(), this._$AH = this.O(t5));
  }
  _(t5) {
    this._$AH !== A && a2(this._$AH) ? this._$AA.nextSibling.data = t5 : this.T(l2.createTextNode(t5)), this._$AH = t5;
  }
  $(t5) {
    const { values: i8, _$litType$: s5 } = t5, e5 = "number" == typeof s5 ? this._$AC(t5) : (void 0 === s5.el && (s5.el = S2.createElement(V(s5.h, s5.h[0]), this.options)), s5);
    if (this._$AH?._$AD === e5) this._$AH.p(i8);
    else {
      const t6 = new R(e5, this), s6 = t6.u(this.options);
      t6.p(i8), this.T(s6), this._$AH = t6;
    }
  }
  _$AC(t5) {
    let i8 = C.get(t5.strings);
    return void 0 === i8 && C.set(t5.strings, i8 = new S2(t5)), i8;
  }
  k(t5) {
    u2(this._$AH) || (this._$AH = [], this._$AR());
    const i8 = this._$AH;
    let s5, e5 = 0;
    for (const h5 of t5) e5 === i8.length ? i8.push(s5 = new _k(this.O(c3()), this.O(c3()), this, this.options)) : s5 = i8[e5], s5._$AI(h5), e5++;
    e5 < i8.length && (this._$AR(s5 && s5._$AB.nextSibling, e5), i8.length = e5);
  }
  _$AR(t5 = this._$AA.nextSibling, s5) {
    for (this._$AP?.(false, true, s5); t5 !== this._$AB; ) {
      const s6 = i3(t5).nextSibling;
      i3(t5).remove(), t5 = s6;
    }
  }
  setConnected(t5) {
    void 0 === this._$AM && (this._$Cv = t5, this._$AP?.(t5));
  }
};
var H = class {
  get tagName() {
    return this.element.tagName;
  }
  get _$AU() {
    return this._$AM._$AU;
  }
  constructor(t5, i8, s5, e5, h5) {
    this.type = 1, this._$AH = A, this._$AN = void 0, this.element = t5, this.name = i8, this._$AM = e5, this.options = h5, s5.length > 2 || "" !== s5[0] || "" !== s5[1] ? (this._$AH = Array(s5.length - 1).fill(new String()), this.strings = s5) : this._$AH = A;
  }
  _$AI(t5, i8 = this, s5, e5) {
    const h5 = this.strings;
    let o5 = false;
    if (void 0 === h5) t5 = M(this, t5, i8, 0), o5 = !a2(t5) || t5 !== this._$AH && t5 !== E, o5 && (this._$AH = t5);
    else {
      const e6 = t5;
      let n4, r4;
      for (t5 = h5[0], n4 = 0; n4 < h5.length - 1; n4++) r4 = M(this, e6[s5 + n4], i8, n4), r4 === E && (r4 = this._$AH[n4]), o5 ||= !a2(r4) || r4 !== this._$AH[n4], r4 === A ? t5 = A : t5 !== A && (t5 += (r4 ?? "") + h5[n4 + 1]), this._$AH[n4] = r4;
    }
    o5 && !e5 && this.j(t5);
  }
  j(t5) {
    t5 === A ? this.element.removeAttribute(this.name) : this.element.setAttribute(this.name, t5 ?? "");
  }
};
var I = class extends H {
  constructor() {
    super(...arguments), this.type = 3;
  }
  j(t5) {
    this.element[this.name] = t5 === A ? void 0 : t5;
  }
};
var L = class extends H {
  constructor() {
    super(...arguments), this.type = 4;
  }
  j(t5) {
    this.element.toggleAttribute(this.name, !!t5 && t5 !== A);
  }
};
var z = class extends H {
  constructor(t5, i8, s5, e5, h5) {
    super(t5, i8, s5, e5, h5), this.type = 5;
  }
  _$AI(t5, i8 = this) {
    if ((t5 = M(this, t5, i8, 0) ?? A) === E) return;
    const s5 = this._$AH, e5 = t5 === A && s5 !== A || t5.capture !== s5.capture || t5.once !== s5.once || t5.passive !== s5.passive, h5 = t5 !== A && (s5 === A || e5);
    e5 && this.element.removeEventListener(this.name, this, s5), h5 && this.element.addEventListener(this.name, this, t5), this._$AH = t5;
  }
  handleEvent(t5) {
    "function" == typeof this._$AH ? this._$AH.call(this.options?.host ?? this.element, t5) : this._$AH.handleEvent(t5);
  }
};
var Z = class {
  constructor(t5, i8, s5) {
    this.element = t5, this.type = 6, this._$AN = void 0, this._$AM = i8, this.options = s5;
  }
  get _$AU() {
    return this._$AM._$AU;
  }
  _$AI(t5) {
    M(this, t5);
  }
};
var j = { M: h2, P: o3, A: n3, C: 1, L: N, R, D: d2, V: M, I: k, H, N: L, U: z, B: I, F: Z };
var B = t2.litHtmlPolyfillSupport;
B?.(S2, k), (t2.litHtmlVersions ??= []).push("3.3.3");
var D = (t5, i8, s5) => {
  const e5 = s5?.renderBefore ?? i8;
  let h5 = e5._$litPart$;
  if (void 0 === h5) {
    const t6 = s5?.renderBefore ?? null;
    e5._$litPart$ = h5 = new k(i8.insertBefore(c3(), t6), t6, void 0, s5 ?? {});
  }
  return h5._$AI(t5), h5;
};

// node_modules/lit-element/lit-element.js
var s3 = globalThis;
var i4 = class extends y {
  constructor() {
    super(...arguments), this.renderOptions = { host: this }, this._$Do = void 0;
  }
  createRenderRoot() {
    const t5 = super.createRenderRoot();
    return this.renderOptions.renderBefore ??= t5.firstChild, t5;
  }
  update(t5) {
    const r4 = this.render();
    this.hasUpdated || (this.renderOptions.isConnected = this.isConnected), super.update(t5), this._$Do = D(r4, this.renderRoot, this.renderOptions);
  }
  connectedCallback() {
    super.connectedCallback(), this._$Do?.setConnected(true);
  }
  disconnectedCallback() {
    super.disconnectedCallback(), this._$Do?.setConnected(false);
  }
  render() {
    return E;
  }
};
i4._$litElement$ = true, i4["finalized"] = true, s3.litElementHydrateSupport?.({ LitElement: i4 });
var o4 = s3.litElementPolyfillSupport;
o4?.({ LitElement: i4 });
(s3.litElementVersions ??= []).push("4.2.2");

// node_modules/lit-html/directive.js
var t3 = { ATTRIBUTE: 1, CHILD: 2, PROPERTY: 3, BOOLEAN_ATTRIBUTE: 4, EVENT: 5, ELEMENT: 6 };
var e4 = (t5) => (...e5) => ({ _$litDirective$: t5, values: e5 });
var i5 = class {
  constructor(t5) {
  }
  get _$AU() {
    return this._$AM._$AU;
  }
  _$AT(t5, e5, i8) {
    this._$Ct = t5, this._$AM = e5, this._$Ci = i8;
  }
  _$AS(t5, e5) {
    return this.update(t5, e5);
  }
  update(t5, e5) {
    return this.render(...e5);
  }
};

// node_modules/lit-html/directive-helpers.js
var { I: t4 } = j;
var i6 = (o5) => o5;
var l3 = (o5, t5) => void 0 === t5 ? void 0 !== o5?._$litType$ : o5?._$litType$ === t5;
var d3 = (o5) => null != o5?._$litType$?.h;
var s4 = () => document.createComment("");
var v2 = (o5, n4, e5) => {
  const l4 = o5._$AA.parentNode, d5 = void 0 === n4 ? o5._$AB : n4._$AA;
  if (void 0 === e5) {
    const i8 = l4.insertBefore(s4(), d5), n5 = l4.insertBefore(s4(), d5);
    e5 = new t4(i8, n5, o5, o5.options);
  } else {
    const t5 = e5._$AB.nextSibling, n5 = e5._$AM, c5 = n5 !== o5;
    if (c5) {
      let t6;
      e5._$AQ?.(o5), e5._$AM = o5, void 0 !== e5._$AP && (t6 = o5._$AU) !== n5._$AU && e5._$AP(t6);
    }
    if (t5 !== d5 || c5) {
      let o6 = e5._$AA;
      for (; o6 !== t5; ) {
        const t6 = i6(o6).nextSibling;
        i6(l4).insertBefore(o6, d5), o6 = t6;
      }
    }
  }
  return e5;
};
var u3 = (o5, t5, i8 = o5) => (o5._$AI(t5, i8), o5);
var m2 = {};
var p3 = (o5, t5 = m2) => o5._$AH = t5;
var M2 = (o5) => o5._$AH;
var h3 = (o5) => {
  o5._$AR(), o5._$AA.remove();
};
var j2 = (o5) => {
  o5._$AR();
};

// node_modules/lit-html/directives/cache.js
var d4 = (t5) => d3(t5) ? t5._$litType$.h : t5.strings;
var h4 = e4(class extends i5 {
  constructor(t5) {
    super(t5), this.et = /* @__PURE__ */ new WeakMap();
  }
  render(t5) {
    return [t5];
  }
  update(s5, [e5]) {
    const u5 = l3(this.it) ? d4(this.it) : null, h5 = l3(e5) ? d4(e5) : null;
    if (null !== u5 && (null === h5 || u5 !== h5)) {
      const e6 = M2(s5).pop();
      let o5 = this.et.get(u5);
      if (void 0 === o5) {
        const s6 = document.createDocumentFragment();
        o5 = D(A, s6), o5.setConnected(false), this.et.set(u5, o5);
      }
      p3(o5, [e6]), v2(o5, void 0, e6);
    }
    if (null !== h5) {
      if (null === u5 || u5 !== h5) {
        const t5 = this.et.get(h5);
        if (void 0 !== t5) {
          const i8 = M2(t5).pop();
          j2(s5), v2(s5, void 0, i8), p3(s5, [i8]);
        }
      }
      this.it = e5;
    } else this.it = void 0;
    return this.render(e5);
  }
});

// node_modules/lit-html/directives/keyed.js
var i7 = e4(class extends i5 {
  constructor() {
    super(...arguments), this.key = A;
  }
  render(r4, t5) {
    return this.key = r4, t5;
  }
  update(r4, [t5, e5]) {
    return t5 !== this.key && (p3(r4), this.key = t5), e5;
  }
});

// node_modules/lit-html/directives/repeat.js
var u4 = (e5, s5, t5) => {
  const r4 = /* @__PURE__ */ new Map();
  for (let l4 = s5; l4 <= t5; l4++) r4.set(e5[l4], l4);
  return r4;
};
var c4 = e4(class extends i5 {
  constructor(e5) {
    if (super(e5), e5.type !== t3.CHILD) throw Error("repeat() can only be used in text expressions");
  }
  dt(e5, s5, t5) {
    let r4;
    void 0 === t5 ? t5 = s5 : void 0 !== s5 && (r4 = s5);
    const l4 = [], o5 = [];
    let i8 = 0;
    for (const s6 of e5) l4[i8] = r4 ? r4(s6, i8) : i8, o5[i8] = t5(s6, i8), i8++;
    return { values: o5, keys: l4 };
  }
  render(e5, s5, t5) {
    return this.dt(e5, s5, t5).values;
  }
  update(s5, [t5, r4, c5]) {
    const d5 = M2(s5), { values: p4, keys: a3 } = this.dt(t5, r4, c5);
    if (!Array.isArray(d5)) return this.ut = a3, p4;
    const h5 = this.ut ??= [], v3 = [];
    let m3, y3, x2 = 0, j3 = d5.length - 1, k2 = 0, w2 = p4.length - 1;
    for (; x2 <= j3 && k2 <= w2; ) if (null === d5[x2]) x2++;
    else if (null === d5[j3]) j3--;
    else if (h5[x2] === a3[k2]) v3[k2] = u3(d5[x2], p4[k2]), x2++, k2++;
    else if (h5[j3] === a3[w2]) v3[w2] = u3(d5[j3], p4[w2]), j3--, w2--;
    else if (h5[x2] === a3[w2]) v3[w2] = u3(d5[x2], p4[w2]), v2(s5, v3[w2 + 1], d5[x2]), x2++, w2--;
    else if (h5[j3] === a3[k2]) v3[k2] = u3(d5[j3], p4[k2]), v2(s5, d5[x2], d5[j3]), j3--, k2++;
    else if (void 0 === m3 && (m3 = u4(a3, k2, w2), y3 = u4(h5, x2, j3)), m3.has(h5[x2])) if (m3.has(h5[j3])) {
      const e5 = y3.get(a3[k2]), t6 = void 0 !== e5 ? d5[e5] : null;
      if (null === t6) {
        const e6 = v2(s5, d5[x2]);
        u3(e6, p4[k2]), v3[k2] = e6;
      } else v3[k2] = u3(t6, p4[k2]), v2(s5, d5[x2], t6), d5[e5] = null;
      k2++;
    } else h3(d5[j3]), j3--;
    else h3(d5[x2]), x2++;
    for (; k2 <= w2; ) {
      const e5 = v2(s5, v3[w2 + 1]);
      u3(e5, p4[k2]), v3[k2++] = e5;
    }
    for (; x2 <= j3; ) {
      const e5 = d5[x2++];
      null !== e5 && h3(e5);
    }
    return this.ut = a3, p3(s5, v3), E;
  }
});

// src/state.js
var state = {
  snap: null,
  audit: [],
  inputs: [],
  watches: [],
  resp: [],
  coordPages: {},
  unboundRows: [],
  unboundAfter: null,
  unboundFull: 0,
  unboundTruncated: false,
  unboundLock: false,
  unboundPages: 0,
  coordList: [],
  coordMap: {},
  coordMeta: null,
  details: /* @__PURE__ */ new Map(),
  statusAt: 0,
  coordAt: 0,
  extraAt: 0,
  statusErr: false,
  coordErr: false,
  extraErr: false,
  detailErr: false,
  errors: {},
  search: "",
  tab: { coordinator: "tasks" },
  scroll: {},
  dirtyForm: false,
  auto: true,
  refreshing: false,
  verifyForms: /* @__PURE__ */ new Map(),
  busyActions: /* @__PURE__ */ new Set(),
  // R2 业务健康/本地人工待办：只保存展示事实，不推导验收/发送结论。
  health: null,
  healthBySid: {},
  healthErr: false,
  healthAt: 0,
  incidents: [],
  incidentStatus: null,
  incidentErr: false,
  // 回合 hold 的只读候选/操作结果；按 session 保存，不据此推断可发送。
  turnCandidates: {},
  // 验收表单草稿：输入事件同步记录，提交读取草稿而非易失 DOM，刷新换节点不丢值。
  verifyDrafts: /* @__PURE__ */ new Map(),
  policyDrafts: /* @__PURE__ */ new Map(),
  policyOperations: /* @__PURE__ */ new Map(),
  prototypeDrafts: /* @__PURE__ */ new Map(),
  prototypeSelected: /* @__PURE__ */ new Map(),
  currentKey: null,
  pendingFocus: null,
  pendingRoute: /* @__PURE__ */ new Set(),
  // 应用初始化标志：boot 完成首轮取数/路由前为 false，详情首屏（deep-link）显示加载而非错误。
  ready: false
};
var listeners = /* @__PURE__ */ new Set();
function subscribe(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}
function publish() {
  for (const listener of listeners) listener(state);
}
var ro = () => !!state.snap?.read_only;
function updateCoordinators(incoming = []) {
  const fresh = new Set(incoming.map((c5) => c5.session));
  const seen = new Set(state.coordList);
  for (const c5 of incoming) {
    if (!seen.has(c5.session)) {
      state.coordList.push(c5.session);
      seen.add(c5.session);
    }
    state.coordMap[c5.session] = { ...state.coordMap[c5.session], ...c5 };
  }
  for (const sid of state.coordList) state.coordMap[sid].missing_snapshot = !fresh.has(sid);
}
function mergeRows(previous = [], incoming = []) {
  const rows = new Map([...previous, ...incoming].map((row) => [row.id, row]));
  return [...rows.values()].sort((a3, b3) => a3.id.localeCompare(b3.id));
}

// src/ui.js
var $2 = (id) => document.getElementById(id);
var ts = (value) => value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "-";
var labels = new Map(Object.entries({
  running: "\u6267\u884C\u4E2D",
  draft: "\u8349\u7A3F",
  pending: "\u5F85\u5904\u7406",
  assigned: "\u5DF2\u5206\u914D",
  paused: "\u5DF2\u6682\u505C",
  blocked: "\u53D7\u963B",
  stalled: "\u8FDB\u5C55\u505C\u6EDE",
  accepted: "\u5DF2\u9A8C\u6536",
  cancelled: "\u5DF2\u53D6\u6D88",
  rejected: "\u672A\u901A\u8FC7",
  result_pending: "\u5F85\u6536\u96C6\u6216\u9A8C\u6536",
  result_received: "\u5DF2\u6536\u5230\u7ED3\u679C",
  result_collected: "\u5DF2\u6536\u96C6\u7ED3\u679C",
  result_missing: "\u7F3A\u5C11\u7ED3\u679C",
  terminal_succeeded: "\u6267\u884C\u6210\u529F",
  terminal_failed: "\u6267\u884C\u5931\u8D25",
  terminal_interrupted: "\u6267\u884C\u4E2D\u65AD",
  verification_failed: "\u9A8C\u6536\u672A\u901A\u8FC7",
  pass: "\u901A\u8FC7",
  fail: "\u672A\u901A\u8FC7",
  unknown: "\u672A\u77E5",
  dispatched: "\u5DF2\u6D3E\u5355",
  queued: "\u5DF2\u6392\u961F",
  sent: "\u5DF2\u53D1\u9001",
  inflight: "\u53D1\u9001\u4E2D",
  acked: "\u5DF2\u786E\u8BA4\u6536\u5230",
  creating: "\u521B\u5EFA\u4E2D",
  dispatch_unknown: "\u6D3E\u5355\u7ED3\u679C\u672A\u77E5",
  blocked_permission: "\u7B49\u5F85\u6743\u9650",
  blocked_form: "\u7B49\u5F85\u586B\u5199\u8868\u5355",
  provider_retry: "\u7B49\u5F85\u670D\u52A1\u5546\u91CD\u8BD5",
  idle_unconfirmed: "\u7A7A\u95F2\u5F85\u786E\u8BA4",
  stop_requested: "\u5DF2\u8BF7\u6C42\u505C\u6B62",
  lost: "\u5931\u53BB\u8054\u7CFB",
  awaiting_review: "\u5F85\u590D\u6838",
  processing: "\u5904\u7406\u4E2D",
  active: "\u6D3B\u52A8\u4E2D",
  ready: "\u5C31\u7EEA",
  dispatching: "\u6D3E\u5355\u4E2D",
  working: "\u6267\u884C\u4E2D",
  waiting_human: "\u7B49\u5F85\u4EBA\u5DE5\u5904\u7406",
  waiting_dependency: "\u7B49\u5F85\u4F9D\u8D56",
  closed: "\u5DF2\u5173\u95ED",
  delivered: "\u5DF2\u9001\u8FBE",
  succeeded: "\u6267\u884C\u6210\u529F",
  acknowledged: "\u5DF2\u786E\u8BA4",
  unacknowledged: "\u5F85\u786E\u8BA4",
  admitted: "\u5DF2\u53D7\u7406",
  recorded: "\u5DF2\u8BB0\u5F55",
  failed: "\u5931\u8D25",
  interrupted: "\u5DF2\u4E2D\u65AD",
  prepared: "\u5DF2\u51C6\u5907",
  possible: "\u53EF\u80FD\u5DF2\u6267\u884C",
  "not-applicable": "\u4E0D\u9002\u7528",
  "manual-blocked": "\u7B49\u5F85\u4EBA\u5DE5\u89E3\u9501",
  exhausted: "\u91CD\u8BD5\u6B21\u6570\u5DF2\u7528\u5B8C",
  cooling: "\u51B7\u5374\u4E2D",
  "probe-unknown": "\u63A2\u6D4B\u7ED3\u679C\u672A\u77E5",
  "probe-ready": "\u7B49\u5F85\u63A2\u6D4B",
  "blocked-unknown": "\u963B\u585E\u539F\u56E0\u672A\u77E5"
}));
var displayStatus = (value) => labels.get(value) ?? (value || "\u672A\u77E5");
var badge = (value) => b2`<span class=${`badge b-${value || "unknown"}`}>${displayStatus(value)}</span>`;
var runLabels = /* @__PURE__ */ new Map([["running", "\u8FD0\u884C\u4E2D"], ["inactive", "\u7A7A\u95F2"], ["unknown", "\u72B6\u6001\u672A\u77E5"]]);
function sessionRunStatus(c5, stale) {
  const status = stale || c5.missing_snapshot || !runLabels.has(c5.session_run_state) ? "unknown" : c5.session_run_state;
  return b2`<span class=${`presence session-run run-${status}`}>${runLabels.get(status)}</span>`;
}
function processStatus(s5, stale) {
  if (stale) return { text: "\u5F85\u66F4\u65B0", tone: "warn" };
  if (s5.alive === false) return { text: "\u672A\u68C0\u6D4B\u5230\u8FDB\u7A0B", tone: "bad" };
  if (s5.healthy === true) return { text: "\u6B63\u5E38", tone: "ok" };
  if (s5.alive === true && s5.healthy === false) return { text: "\u5FC3\u8DF3\u5F02\u5E38", tone: "warn" };
  if (s5.healthy === false) return { text: "\u5F02\u5E38", tone: "bad" };
  return { text: "\u72B6\u6001\u672A\u77E5", tone: "muted" };
}
function connectionStatus(s5, stale) {
  if (stale) return "\u5F85\u66F4\u65B0";
  if (s5.alive === false) return "\u672A\u8FDE\u63A5";
  if (s5.healthy !== true) return "\u672A\u77E5";
  const stream = s5.stream || {};
  if (stream.backoff > 0) return "\u6B63\u5728\u91CD\u8BD5";
  if (s5.connected === true) return stream.backoff === 0 ? "\u5DF2\u8FDE\u63A5" : "\u66FE\u5EFA\u7ACB\u8FDE\u63A5";
  if (s5.connected === false && stream.connects > 0) return "\u8FDE\u63A5\u4E2D";
  return s5.connected === false ? "\u5C1A\u672A\u8FDE\u63A5" : "\u672A\u77E5";
}
function fmtAge(ms) {
  if (ms == null || isNaN(Number(ms))) return "-";
  const s5 = Math.max(0, Math.floor(Number(ms) / 1e3));
  if (s5 < 60) return `${s5} \u79D2`;
  if (s5 < 3600) return `${Math.floor(s5 / 60)} \u5206\u949F`;
  if (s5 < 86400) return `${Math.floor(s5 / 3600)} \u5C0F\u65F6`;
  return `${Math.floor(s5 / 86400)} \u5929`;
}
var toastTimer;
function toast(message) {
  const node = $2("toast");
  if (!node) return;
  node.textContent = message;
  node.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => node.classList.remove("show"), 2e3);
}
async function copyText(text) {
  if (!text) return;
  try {
    if (navigator.clipboard?.writeText) await navigator.clipboard.writeText(text);
    else {
      const active = document.activeElement;
      const node = document.createElement("textarea");
      node.value = text;
      node.style.cssText = "position:fixed;opacity:0";
      document.body.append(node);
      node.select();
      try {
        if (!document.execCommand("copy")) throw new Error("copy");
      } finally {
        node.remove();
        active?.focus({ preventScroll: true });
      }
    }
    toast("\u5DF2\u590D\u5236 " + text.slice(0, 40));
  } catch {
    toast("\u590D\u5236\u5931\u8D25\uFF0C\u8BF7\u624B\u52A8\u9009\u62E9");
  }
}
function markDirty() {
  state.dirtyForm = state.verifyForms.size > 0 || !!document.querySelector('[data-dirty="true"]');
}
function gateBadge(gate) {
  return gate?.state ? b2`<span class=${`badge b-${gate.state === "manual-blocked" ? "expired" : "blocked"}`}>${displayStatus(gate.state)}</span>` : A;
}
function unlockActionFor(coord) {
  const project = (coord.leases || [])[0]?.project || (coord.projects || [])[0];
  if (!project) return null;
  const manual2 = (coord.gates || []).find((g2) => g2.state === "manual-blocked");
  if (manual2) return { label: "\u89E3\u9664\u4EBA\u5DE5\u963B\u585E", project, allowProvider: false };
  const blocking = (coord.gates || []).find((g2) => g2.source === "provider" && ["blocked", "exhausted", "probe-unknown"].includes(g2.state));
  if (blocking) return { label: "\u6062\u590D\u8C03\u5EA6", project, allowProvider: true };
  return null;
}
function identity(coordinator) {
  const confirmed = coordinator.identity === "confirmed" || (coordinator.sources || []).some((s5) => s5.startsWith("task:") || s5.startsWith("lease:"));
  const notifyOnly = !confirmed && (coordinator.sources || []).length > 0 && coordinator.sources.every((s5) => s5.startsWith("notify:"));
  return {
    badge: confirmed ? b2`<span class="badge b-valid">已关联调度者</span>` : notifyOnly ? b2`<span class="badge b-blocked">仅通知关联 · 调度者角色未确认</span>` : b2`<span class="badge">身份未知</span>`,
    name: coordinator.title?.trim() || (notifyOnly ? "\u901A\u77E5\u5173\u8054\u4F1A\u8BDD" : coordinator.session || "\u672A\u547D\u540D\u4F1A\u8BDD")
  };
}
var copyButton = (id, label = "\u590D\u5236 ID") => b2`<button class="copy" data-copy=${id} aria-label=${label}>${label}</button>`;
var busy = (key) => state.busyActions.has(key);
var verifySubmissionNote = "\u6309\u6807\u51C6\u9A8C\u6536\uFF0C\u63D0\u4EA4\u65F6\u4F1A\u6838\u5BF9\u4EA4\u4ED8\u8BC1\u636E\u3002";
var MANAGED_VALUES = {
  ok: ["delivered", "succeeded", "acknowledged", "admitted", "recorded"],
  blocked: ["prepared", "possible", "pending", "unknown", "unacknowledged", "not-applicable"],
  bad: ["cancelled", "failed", "interrupted"]
};
function managedBadge(value) {
  if (value == null || value === "") return b2`<span class="muted">-</span>`;
  const s5 = String(value);
  if (MANAGED_VALUES.ok.includes(s5)) return b2`<span class="badge b-valid">${displayStatus(s5)}</span>`;
  if (MANAGED_VALUES.blocked.includes(s5)) return b2`<span class="badge b-blocked">${displayStatus(s5)}</span>`;
  if (MANAGED_VALUES.bad.includes(s5)) return b2`<span class="badge b-expired">${displayStatus(s5)}</span>`;
  return b2`<span class="badge">${displayStatus(s5)}</span>`;
}
function watchEnabledBadge(v3) {
  if (v3 === true) return b2`<span class="badge b-valid">已启用</span>`;
  if (v3 === false) return b2`<span class="badge b-expired">已停用</span>`;
  return b2`<span class="muted">-</span>`;
}
function obsTrustBadge(v3) {
  if (v3 === true) return b2`<span class="badge b-valid">可信</span>`;
  if (v3 === false) return b2`<span class="badge b-expired">未获信任</span>`;
  return b2`<span class="badge b-blocked">未知</span>`;
}
function watchObservation(w2) {
  const obs = w2.last_observation;
  if (!obs || typeof obs !== "object" || !Object.keys(obs).length) return b2`<span class="muted">尚无观察记录</span>`;
  const bits = [
    b2`待处理 ${obs.pending == null ? "-" : obs.pending}`,
    b2` · 最长等待 ${obs.oldest_age_ms == null ? "-" : fmtAge(obs.oldest_age_ms)}`,
    obsTrustBadge(obs.trusted)
  ];
  if (obs.aged_out === true) bits.push(b2`<span class="badge b-blocked">已超时</span>`);
  if (obs.stale === true) bits.push(b2`<span class="badge b-blocked">已过期</span>`);
  if (obs.blocking_reason != null) bits.push(b2`<span class="muted">受阻原因 ${obs.blocking_reason}</span>`);
  if (w2.last_observed_at) bits.push(b2`<div class="muted">观察时间 ${ts(w2.last_observed_at)}</div>`);
  if (obs.next_action) bits.push(b2`<div class="muted">下一步 ${obs.next_action}</div>`);
  return b2`${bits}`;
}

// src/views/overview.js
function coordCard(c5) {
  const ident = identity(c5);
  const counts = c5.counts || {};
  const href = `#/coordinator/${encodeURIComponent(c5.session)}`;
  const unlock2 = unlockActionFor(c5);
  const tips = (c5.task_tips || []).slice(0, 3);
  const flagged = c5.attention && c5.attention_rank < 6;
  return b2`<article class="card coord-card" data-key=${`coordinator:${c5.session}`}>
    <div class="session-heading">${ident.badge}${sessionRunStatus(c5, state.coordErr)}</div>
    <h3><a href=${href}>${ident.name}</a></h3>
    <div class="project-line">${(c5.projects || []).join("\u3001") || "\u672A\u5173\u8054\u9879\u76EE"}</div>
    ${c5.missing_snapshot ? b2`<div class="warn">本次未找到该会话，显示上次记录</div>` : A}
    <dl class="card-metrics">
      <div><dt>未完成</dt><dd>${counts.unfinished ?? "\u2014"}</dd></div>
      <div><dt>待验收</dt><dd>${counts.awaiting_verification ?? "\u2014"}</dd></div>
      <div><dt>受阻</dt><dd class=${counts.blocked ? "bad" : ""}>${counts.blocked ?? "\u2014"}</dd></div>
    </dl>
    ${flagged ? b2`<div class=${`attention-line ${c5.attention_rank <= 1 ? "bad" : "warn"}`}>${c5.attention}</div>` : A}
    <details class="card-details"><summary>会话详情</summary>
      <dl class="kv">
        <dt>会话 ID</dt><dd><code class="mono">${c5.session}</code></dd>
        <dt>关联依据</dt><dd>${(c5.sources || []).join("\u3001") || "\u672A\u77E5"}</dd>
        <dt>创建时间</dt><dd>${c5.created_at_known ? ts(c5.created_at) : "\u521B\u5EFA\u65F6\u95F4\u672A\u77E5"}</dd>
        ${c5.created_meta_stale || c5.metadata_fresh === false ? b2`<dt>会话信息</dt><dd class="warn">尚未更新 ${c5.created_meta_error?.split(":")[0] || ""}</dd>` : A}
        <dt>最近进展</dt><dd>${ts(c5.last_update)}</dd>
        <dt>项目租约</dt><dd>${(c5.leases || []).length ? c5.leases.map((l4) => b2`<div>${l4.project} · ${l4.valid ? "\u6709\u6548" : "\u5DF2\u5230\u671F"} ${l4.recovery_blocked ? "\u6062\u590D\u53D7\u963B" : l4.failure_count ? `\u5931\u8D25 ${l4.failure_count} \u6B21` : ""}</div>`) : "\u6682\u65E0"}</dd>
        <dt>调度限制</dt><dd>${(c5.gates || []).length ? c5.gates.map(gateBadge) : "\u6682\u65E0"}</dd>
        <dt>其他记录</dt><dd>已验收 ${counts.accepted ?? "\u2014"} · 已取消 ${counts.cancelled ?? "\u2014"} · 待处理通知 ${counts.notifications_pending ?? "\u2014"}</dd>
      </dl>
      ${tips.map((t5) => b2`<div class="task-line">${badge(t5.status)}<a href=${`#/task/${encodeURIComponent(t5.id)}`}>${t5.title || t5.id}</a></div>`)}
      ${counts.unfinished > tips.length || c5.task_tips_truncated ? b2`<div class="note">其余任务可在工作台查看。</div>` : A}
    </details>
    <div class="card-footer"><a class="button workspace-link" href=${href}>查看工作台 <span aria-hidden="true">→</span></a>
      ${unlock2 ? b2`<button class="danger" data-act="unlock" data-sid=${c5.session} data-project=${unlock2.project} data-allow-provider=${unlock2.allowProvider ? "1" : "0"} ?disabled=${ro()}>${unlock2.label}</button>` : A}
      ${copyButton(c5.session)}</div>
  </article>`;
}
function summaryLink(href, label, count) {
  return b2`<a class="summary-link" href=${href}><span>${label}</span>
    <strong>${count ?? "\u2014"}</strong><span class="summary-arrow" aria-hidden="true">↗</span></a>`;
}
function renderOverview() {
  const meta = state.coordMeta;
  const departed = state.coordList.filter((sid) => state.coordMap[sid].missing_snapshot).length;
  const outside = meta && state.ready && !state.errors.responsibilities && !state.coordErr ? state.resp.filter((r4) => !state.coordMap[r4.owner]).length : null;
  const alerts = state.snap?.alerts?.filter((a3) => !a3.task_id && !a3.attempt_id).length;
  return b2`<div class="page-heading"><h2>调度总览</h2></div>
    <div class="summary-band">
      ${summaryLink("#/tasks/unbound", "\u5F85\u5206\u914D\u4EFB\u52A1", meta?.unbound_task_count)}
      ${summaryLink("#/diagnostics/responsibilities", "\u5176\u4ED6\u8D1F\u8D23\u4EBA\u5F85\u529E", outside)}
      ${summaryLink("#/diagnostics/alerts", "\u672A\u5173\u8054\u4EFB\u52A1\u7684\u544A\u8B66", alerts)}
    </div>
    <div class="section-heading"><h3>调度会话 <span class="count">${meta ? state.coordList.length : "\u2014"}</span></h3></div>
    <div class="grid-cards">${state.coordList.length ? c4(state.coordList, (sid) => sid, (sid) => coordCard(state.coordMap[sid])) : b2`<div class="empty">${meta ? "\u6682\u65E0\u5173\u8054\u7684\u8C03\u5EA6\u4F1A\u8BDD\u3002" : "\u6682\u65F6\u65E0\u6CD5\u8BFB\u53D6\u8C03\u5EA6\u4F1A\u8BDD\u3002"}</div>`}</div>
    <details class="scope"><summary>数据范围与排序</summary>
      <p>待分配任务按全部任务统计。其他负责人待办统计当前读取的最多 200 条待办中，由列表以外的负责人处理的事项。告警统计本次读取的最多 50 条新告警。</p>
      <p>会话按创建时间从早到晚排列。仅收到通知的会话会标注身份。创建时间未知的排在最后，同组按会话 ID 排列。刷新时保留原有顺序。</p>
      ${departed ? b2`<p>其中 ${departed} 个会话仅有上次记录。</p>` : A}
      ${meta?.metadata_fresh === false ? b2`<p class="warn">部分会话缺少最新信息。</p>` : A}
    </details>`;
}

// src/views/coordinator.js
function tasksTab(sid, pag) {
  const tasks = pag.tasks || [];
  const unfinished = tasks.filter((t5) => t5.status !== "accepted" && t5.status !== "cancelled");
  const accepted = tasks.filter((t5) => t5.status === "accepted");
  const cancelled = tasks.filter((t5) => t5.status === "cancelled");
  const trow = (t5) => b2`<div class="task-line coord-task">
    <div class="row">${badge(t5.status)}<a class="title" href=${`#/task/${encodeURIComponent(t5.id)}`}>${t5.title || t5.id}</a></div>
    <div class="task-context"><span>${t5.project || "\u9879\u76EE\u672A\u77E5"} · ${t5.owned_via === "explicit" ? "\u4EFB\u52A1\u5355\u72EC\u6307\u5B9A" : "\u7531\u9879\u76EE\u8C03\u5EA6\u8005\u8D1F\u8D23"}</span>
      ${t5.current_attempt ? b2`<a href=${`#/attempt/${encodeURIComponent(t5.current_attempt)}`}>查看本次执行</a>` : A}</div>
    ${t5.blocked_reason ? b2`<div class="warn">受阻原因 · ${t5.blocked_reason}</div>` : A}
  </div>`;
  const histRow = (label) => (t5) => b2`<div class="task-line"><span>${badge(t5.status)}</span><a href=${`#/task/${encodeURIComponent(t5.id)}`}>${t5.title || t5.id}</a><span class="spacer"></span><span class="note">${label} ${ts(t5.updated_at)}</span></div>`;
  const acc = c4(accepted.slice(0, 20), (t5) => t5.id, histRow("\u5DF2\u9A8C\u6536"));
  const can = c4(cancelled.slice(0, 20), (t5) => t5.id, histRow("\u5DF2\u53D6\u6D88"));
  return b2`<h3>未完成任务 <span class="count">${unfinished.length}</span></h3><div class="note">已加载 ${tasks.length} 项，共 ${pag.full} 项</div>
    ${unfinished.length ? c4(unfinished, (t5) => t5.id, trow) : b2`<div class="empty">当前列表中没有未完成任务。</div>`}
    ${accepted.length ? b2`<h3>已验收 · ${accepted.length}</h3>${acc}${accepted.length > 20 ? b2`<div class="note">… 已加载其中前 20 项，其余请使用任务检索</div>` : A}` : A}
    ${cancelled.length ? b2`<h3>已取消 · ${cancelled.length}</h3>${can}${cancelled.length > 20 ? b2`<div class="note">… 已加载其中前 20 项，其余请使用任务检索</div>` : A}` : A}
    ${pag.truncated ? b2`<div class="op-panel"><button class="primary" data-act="coordmore" data-sid=${sid} ?disabled=${pag.lock}>继续加载</button></div>` : A}
`;
}
function todosTab(notes, resp) {
  const rrow = (r4) => b2`<div class="task-line"><span>${badge(r4.state)}</span><span class="mono">${r4.id || "-"}</span><div><div>${r4.owner || "-"} → ${r4.target_session || "-"}</div><div class="note">等待条件：${r4.wait_condition || "-"} · 下一步 ${r4.next_action || "-"} · 期限 ${ts(r4.deadline)}</div></div></div>`;
  const actionable = notes.filter((n4) => ["pending", "inflight", "sent"].includes(n4.state));
  return b2`<h3>待办事项</h3>
    ${resp.length ? c4(resp, (r4) => r4.id || "", rrow) : b2`<div class="empty">暂无待办事项。</div>`}
    ${actionable.length ? b2`<h3>待处理通知</h3>${c4(actionable, (n4) => n4.id, (n4) => b2`<div class="task-line"><span>${badge(n4.state)}</span><span class="mono">${n4.id}</span><span>${n4.kind} · ${n4.summary || ""}</span><span class="spacer"></span>${ro() ? A : b2`<button class="primary" data-act="acknote" data-id=${n4.id}>确认收到</button>`}</div>`)}` : A}`;
}
function activityTab(sid, notes) {
  const nrow = (n4) => b2`<div class="task-line"><span>${badge(n4.state)}</span><span class="mono">${n4.id}</span><span>${n4.kind}</span><div class="note">摘要：${n4.summary || "-"} · 发送于 ${ts(n4.sent_at)}</div><span class="spacer"></span></div>`;
  const inputs = (state.inputs || []).filter((i8) => (i8.session_id || i8.session) === sid);
  return b2`<h3>通知记录</h3>
    ${notes.length ? c4(notes, (n4) => n4.id, nrow) : b2`<div class="empty">无通知记录。</div>`}
    <h3>发给此会话的指令</h3>
    ${inputs.length ? c4(inputs, (i8) => i8.input_id || i8.message_id || "", (i8) => b2`<div class="task-line"><span class="mono">${i8.input_id || "-"}</span><span>${i8.intent || "-"} · 受理 ${i8.admission || "-"} → 投递 ${i8.delivery || "-"} → 业务ACK ${i8.business_ack || "-"}</span><span class="note">下一步 ${i8.next_action || "-"}</span></div>`) : b2`<div class="empty">无输入回执。</div>`}`;
}
function healthTab(sid, data) {
  const h5 = state.healthBySid[sid];
  const body = h5?.body;
  const tasks = body?.tasks || [];
  const projects = data.coordinator?.projects || [];
  const single = projects.length === 1 ? projects[0] : null;
  const r4 = ro();
  const controlTask = tasks.find((t5) => t5.project === single && t5.control);
  const intent = controlTask?.control?.project_intent;
  const rev = controlTask?.control?.project_revision;
  const epoch = controlTask?.control?.epoch;
  const hasControlVersion = typeof rev === "number" && typeof epoch === "number";
  const showResume = !r4 && single && intent === "paused" && hasControlVersion;
  const showPause = !r4 && single && intent !== "paused" && hasControlVersion;
  const controlBits = (t5) => {
    const c5 = t5.control || {};
    const bits = [
      c5.project_intent ? `\u610F\u56FE ${c5.project_intent}` : null,
      c5.turn_held ? "hold" : null,
      c5.user_turn_active ? "\u56DE\u5408\u4E2D" : null,
      c5.provider_gate ? `provider ${c5.provider_gate}` : null
    ].filter(Boolean).join(" \xB7 ");
    const ver = c5.project_revision != null ? `rev ${c5.project_revision}` : "";
    const ep = c5.epoch != null ? `epoch ${c5.epoch}` : "";
    const blocking = (c5.blocking || []).join("\u3001");
    return b2`${bits || b2`<span class="muted">无控制记录</span>`}${ver || ep ? b2`<div class="muted">${ver}${ver && ep ? " \xB7 " : ""}${ep}</div>` : A}${blocking ? b2`<div class="bad">${blocking}</div>` : A}`;
  };
  const trow = (t5) => {
    const resp = (t5.responsibilities || [])[0] || {};
    return b2`<tr>
      <td><a href=${`#/task/${encodeURIComponent(t5.task_id || "")}`}><code class="mono">${t5.task_id || "-"}</code></a><div class="muted">${t5.project || t5.coordinator_session || ""}</div></td>
      <td>${badge(t5.state)}${t5.process_healthy != null ? b2`<div class="muted">proc ${t5.process_healthy ? "ok" : "down"}</div>` : A}</td>
      <td class="mono">${t5.owner || "-"}</td>
      <td>${t5.next_action || "-"}</td>
      <td>${resp.deadline != null ? ts(resp.deadline) : "-"}<div class="muted">下一步 ${resp.next_check_at != null ? ts(resp.next_check_at) : "-"}</div></td>
      <td>${resp.budget_used != null ? `${resp.budget_used}/${resp.max_budget ?? "-"}` : "-"}</td>
      <td>${t5.evidence_coverage === true ? b2`<span class="ok">完整</span>` : t5.evidence_coverage === false ? b2`<span class="warn">不足</span>` : b2`<span class="muted">-</span>`}</td>
      <td>${ts(t5.latest_business_event_at)}</td>
      <td>${controlBits(t5)}</td>
    </tr>`;
  };
  const proc = body?.process || {};
  const procLine = proc.healthy == null ? b2`<span class="muted">进程状态未知</span>` : proc.healthy ? b2`<span class="ok">进程正常</span> ${proc.connected != null ? `connected=${proc.connected}` : ""}${proc.heartbeat_age_s != null ? b2` · 心跳 ${proc.heartbeat_age_s}s 前` : ""}${proc.last_cycle_at != null ? b2` · 上轮 ${ts(proc.last_cycle_at)}` : ""}` : b2`<span class="bad">进程异常</span>`;
  const runtime = body?.runtime || {};
  const runtimeBits = [];
  if (runtime.release_id) runtimeBits.push(b2`release ${runtime.release_id}`);
  if (runtime.manifest_verified === true) runtimeBits.push(b2`<span class="ok">manifest 校验通过</span>`);
  else if (runtime.source) runtimeBits.push(b2`<span class="muted">${runtime.source}</span>`);
  if (runtime.package_version) runtimeBits.push(b2`包版本 ${runtime.package_version}`);
  if (runtime.email_default_enabled === false) runtimeBits.push(b2`邮件默认关闭`);
  if (runtime.native_trusted_chat_enabled === false) runtimeBits.push(b2`native 对话未启用`);
  const scopeNote = body?.coverage ? body.coverage.complete ? "\u8986\u76D6\u5B8C\u6574" : `\u90E8\u5206\u8986\u76D6${body.coverage.next_after_id ? "\uFF0C\u53EF\u7EED\u9875" : ""}` : "\u8986\u76D6\u672A\u77E5";
  const ctrl = !single ? b2`<div class="note">${projects.length ? "\u6B64\u4F1A\u8BDD\u8D1F\u8D23\u591A\u4E2A\u9879\u76EE\uFF0C\u8BF7\u6309\u9879\u76EE\u9009\u62E9\u6682\u505C\u6216\u6062\u590D\u3002" : "\u6B64\u4F1A\u8BDD\u5C1A\u672A\u5173\u8054\u9879\u76EE\u3002"}</div>` : b2`<div class="row" style="margin:6px 0">
        <span class="muted">项目 <code class="mono">${single}</code>：${intent ?? "\u672A\u77E5"}</span>${hasControlVersion ? b2`<span class="muted"> · rev ${rev} · epoch ${epoch}</span>` : A}<span class="spacer"></span>
        ${showResume ? b2`<button class="primary" data-act="projresume" data-sid=${sid} data-project=${single} ?disabled=${r4}>继续</button>` : A}
        ${showPause ? b2`<button class="danger" data-act="projpause" data-sid=${sid} data-project=${single} ?disabled=${r4}>暂停</button>` : A}
        ${!hasControlVersion && single ? b2`<span class="muted">缺少项目版本，暂时无法操作</span>` : A}
      </div>`;
  return b2`<div id="bizHealth">
    <h3>业务健康</h3>
    <div class="note">任务完成以验收结论为准。 ${scopeNote}${h5?.error ? b2` <span class="bad">刷新失败，显示上次记录 · ${h5.error.message}</span>` : A}</div>
    <div class="note">${procLine}</div>
    ${ctrl}
    <div class="tablewrap"><table><thead><tr><th>任务</th><th>状态</th><th>负责人</th><th>下一动作</th><th>期限 / 下次检查</th><th>预算</th><th>证据</th><th>最近业务事件</th><th>调度限制</th></tr></thead><tbody>
      ${tasks.length ? c4(tasks, (t5) => t5.task_id || JSON.stringify(t5), trow) : b2`<tr><td colspan="9" class="muted">${h5 ? "\u8BE5\u4F1A\u8BDD\u5F53\u524D\u65E0\u672A\u5B8C\u6210\u4E1A\u52A1\u5065\u5EB7\u8BB0\u5F55\u3002" : "\u6B63\u5728\u7B49\u5F85\u5065\u5EB7\u6570\u636E\u3002"}</td></tr>`}
    </tbody></table></div>
    ${runtimeBits.length ? b2`<div class="note" id="runtimeIdentity">运行身份：${runtimeBits}</div>` : A}
  </div>`;
}
function renderCoordinator(sid, data = {}) {
  const c5 = data.coordinator || {};
  const key = `coordinator:${sid}`;
  const stale = state.details.get(key)?.error || state.pendingRoute.has(key);
  const ident = identity(c5);
  const pag = state.coordPages[sid] || {
    tasks: data.tasks || [],
    full: data.tasks_full_count ?? (data.tasks || []).length,
    after: data.next_after_id || null,
    truncated: !!data.tasks_truncated,
    lock: false
  };
  const tab = state.tab.coordinator || "tasks";
  const created = c5.created_at_known ? ts(c5.created_at) : b2`<span class="warn">创建时间未知</span>`;
  const leases = (c5.leases || []).length ? c4(c5.leases, (l4) => l4.project || "", (l4) => b2`<div class=${l4.valid ? "ok" : "bad"}>${l4.project || ""}：${l4.valid ? "\u79DF\u7EA6\u6709\u6548" : "\u5DF2\u5230\u671F"}${l4.recovery_blocked ? b2` <span class="bad">恢复阻塞</span>` : l4.failure_count ? b2` <span class="warn">失败 ${l4.failure_count}</span>` : A}<span class="muted"> 到期 ${ts(l4.expires_at)}</span></div>`) : b2`<span class="muted">无项目租约</span>`;
  const gates = (c5.gates || []).length ? c4(c5.gates, (g2) => `${g2.state}:${g2.source || ""}`, (g2) => b2`<div class=${g2.state === "manual-blocked" ? "bad" : "warn"}>${gateBadge(g2)}${g2.reason ? b2`<div class="muted">${g2.reason}</div>` : A}</div>`) : b2`<span class="muted">暂无调度限制</span>`;
  const unlockAction = unlockActionFor(c5);
  const unlock2 = unlockAction ? b2`<button class="danger" data-act="unlock" data-sid=${sid} data-project=${unlockAction.project} data-allow-provider=${unlockAction.allowProvider ? "1" : "0"} ?disabled=${ro()}>${unlockAction.label}</button>` : A;
  const counts = c5.counts || {};
  const countsRow = Object.keys(counts).length ? b2`未完成 ${counts.unfinished ?? 0} · 待验收 ${counts.awaiting_verification ?? 0} · 受阻 ${counts.blocked ?? 0} · 已验收 ${counts.accepted ?? 0} · 已取消 ${counts.cancelled ?? 0} · 通知 ${counts.notifications_pending ?? 0}` : "-";
  const sc = data.session_control || {};
  const hold = sc.hold;
  const barrier = sc.barrier;
  const singleProject = (c5.projects || []).length === 1 ? c5.projects[0] : null;
  const candidate = state.turnCandidates[sid] && state.turnCandidates[sid].revision === sc.revision ? state.turnCandidates[sid] : null;
  const holdRow = sc.ok === false ? b2`<span class="bad">回合控制不可读 · ${sc.error || "\u672A\u77E5\u9519\u8BEF"}</span>` : hold ? b2`<div class="bad">自动唤醒已暂停 · ${hold.reason || "unknown"}<span class="muted"> ${ts(hold.held_at)}</span></div>${barrier ? b2`<div class="warn">正在等待用户回合结束 · <code class="mono">${barrier.message_id || "-"}</code> · ${ts(barrier.origin_at)}</div>` : b2`<div class="muted">暂无待结束的用户回合</div>`}` : b2`<span class="muted">自动唤醒未暂停</span>`;
  const candidateRow = candidate ? candidate.eligible ? b2`<div class="ok">可登记的用户输入：<code class="mono">${candidate.message_id}</code> · ${ts(candidate.created)} · ${candidate.outcome}</div>` : b2`<div class="warn">不可登记：${candidate.reason || "unknown"}${candidate.message_id ? b2` · 最新用户输入 <code class="mono">${candidate.message_id}</code>` : A}</div>` : A;
  const holdActions = hold && !ro() && singleProject && !barrier && sc.ok !== false ? b2`<div class="op-panel">
        <button data-act="checkturn" data-sid=${sid} data-project=${singleProject}>检查可登记的用户回合</button>
        ${candidate?.eligible ? b2`<button class="primary" data-act="registerturn" data-sid=${sid} data-project=${singleProject}>登记已完成的回合</button>` : A}
        <button class="danger" data-act="releasehold" data-sid=${sid} data-project=${singleProject}>强制恢复自动唤醒</button>
        <span class="note">登记需要已完成的用户输入。强制恢复后，系统仍会检查项目暂停和其他限制。</span>
      </div>
      ${candidateRow}` : A;
  const body = tab === "tasks" ? tasksTab(sid, pag) : tab === "todos" ? todosTab(data.notifications || [], data.responsibilities || []) : tab === "health" ? healthTab(sid, data) : activityTab(sid, data.notifications || []);
  return b2`<div class="breadcrumb"><a href="#/">总览</a> <span class="sep">/</span> <span>${ident.name}</span></div>
    <div class="row workspace-heading"><h2>${ident.name}</h2>${sessionRunStatus(c5, stale)}${ident.badge}<span class="spacer"></span>${copyButton(sid)}${unlock2}</div>
    ${(c5.projects || []).length === 1 && !ro() ? b2`<div class="op-panel">
      <button data-act="usersend" data-sid=${sid} data-project=${c5.projects[0]}>补充指令</button>
      <button data-act="userobserve" data-sid=${sid} data-project=${c5.projects[0]}>检查上一条指令</button>
</div>` : A}
    <details class="scope"><summary>会话与项目详情</summary><dl class="kv">
      <dt>会话 ID</dt><dd><code class="mono">${sid}</code></dd>
      <dt>创建时间</dt><dd>${created}</dd>
      <dt>项目</dt><dd>${(c5.projects || []).length ? (c5.projects || []).join("\u3001") : b2`<span class="muted">-</span>`}</dd>
      <dt>租约</dt><dd>${leases}</dd>
      <dt>调度限制</dt><dd>${gates}</dd>
      <dt>全部任务与通知</dt><dd>${countsRow}</dd>
    </dl></details>
    ${hold || sc.ok === false ? b2`<div class="attn rank-mid"><strong>回合控制</strong>${holdRow}</div>` : A}
    ${holdActions}
    <div class="tabs" role="tablist" aria-label="工作台分区">
      <button role="tab" aria-selected=${tab === "tasks" ? "true" : "false"} data-tab="tasks">任务</button>
      <button role="tab" aria-selected=${tab === "todos" ? "true" : "false"} data-tab="todos">待办</button>
      <button role="tab" aria-selected=${tab === "health" ? "true" : "false"} data-tab="health">健康</button>
      <button role="tab" aria-selected=${tab === "activity" ? "true" : "false"} data-tab="activity">活动</button>
    </div>
    ${body}`;
}

// src/views/forms.js
function renderVerifyForm(key) {
  const entry = state.verifyForms.get(key);
  if (!entry) return A;
  const attemptId = entry.attemptId;
  const pass = Boolean(entry.pass);
  const locked = ro() || busy(`verify:${attemptId}`);
  const draft = state.verifyDrafts.get(key) || {};
  const v3 = (name) => draft[name] ?? "";
  return i7(`${attemptId}:${pass ? 1 : 0}`, b2`<form data-form="verify" data-key=${key} data-id=${attemptId} data-pass=${pass ? 1 : 0} class="card">
    <legend>${pass ? "\u901A\u8FC7\u9A8C\u6536" : "\u9A8C\u6536\u4E0D\u901A\u8FC7"} · <code class="mono">${attemptId}</code></legend>
    <label>证据路径，多个用英文分号隔开<input type="text" name="evidence" .value=${v3("evidence")} ?disabled=${locked}></label>
    <label>备注 · 可选<input type="text" name="notes" .value=${v3("notes")} ?disabled=${locked}></label>
    <label>${pass ? "\u901A\u8FC7\u539F\u56E0 \xB7 \u53EF\u9009" : "\u672A\u901A\u8FC7\u539F\u56E0 \xB7 \u5FC5\u586B"}<input type="text" name="reason" .value=${v3("reason")} ?required=${!pass} ?disabled=${locked}></label>
    <div class="note">${verifySubmissionNote}</div>
    <label><input type="checkbox" name="run_checks" ?disabled=${locked}> 提交前运行验收检查</label>
    <label>确认：输入尝试 ID <input type="text" name="confirm" placeholder=${attemptId} .value=${v3("confirm")} required ?disabled=${locked}></label>
    <div class="op-panel"><button class="primary" type="submit" ?disabled=${locked}>${pass ? "\u786E\u8BA4\u901A\u8FC7\u9A8C\u6536" : "\u786E\u8BA4\u9A8C\u6536\u4E0D\u901A\u8FC7"}</button><button type="button" data-act="closeverify" data-key=${key}>取消</button></div>
  </form>`);
}
function renderReworkForm(taskId) {
  const locked = ro();
  return b2`<form data-form="rework" data-id=${taskId} class="card">
    <legend>返工/继续：<code class="mono">${taskId}</code></legend>
    <label>返工要求 · 必填<textarea name="feedback" required ?disabled=${locked}></textarea></label>
    <label>工作模式<select name="work_mode" ?disabled=${locked}>
      <option value="latency-first">时效优先</option>
      <option value="budget-first">预算优先</option>
      <option value="elastic">弹性加速</option>
    </select></label>
    <div class="note">工作模式用于指导本次返工。模型和费用按现有授权执行。</div>
    <label>确认：输入任务 ID <input type="text" name="confirm" placeholder=${taskId} required ?disabled=${locked}></label>
    <div class="op-panel"><button class="primary" type="submit" ?disabled=${locked}>提交返工</button></div>
  </form>`;
}

// src/policy-storage.js
var prefix = "oc-orch-policy-pending:v1:";
function pendingPolicy(key) {
  if (state.policyOperations.has(key)) return state.policyOperations.get(key);
  try {
    const raw = window.sessionStorage.getItem(prefix + key);
    if (!raw) return null;
    const value = JSON.parse(raw);
    if (typeof value?.body?.operation_id !== "string" || `${value.scope}:${value.target}` !== key) {
      return { body: { operation_id: "invalid-local-pending-record" }, invalid: true };
    }
    return value;
  } catch {
    return { body: { operation_id: "pending-record-unreadable" }, invalid: true };
  }
}
function savePendingPolicy(key, value) {
  window.sessionStorage.setItem(prefix + key, JSON.stringify(value));
  state.policyOperations.set(key, value);
}
function clearPendingPolicy(key) {
  window.sessionStorage.removeItem(prefix + key);
  state.policyOperations.delete(key);
}

// src/views/delivery-policy.js
var modes = [["budget-first", "\u9884\u7B97\u4F18\u5148"], ["elastic", "\u5F39\u6027\u52A0\u901F"], ["latency-first", "\u65F6\u6548\u4F18\u5148"]];
function policyForm(view, scope, target) {
  const key = `${scope}:${target}`;
  const draft = state.policyDrafts.get(key) || {};
  const p4 = view.version?.policy || { preset: "budget-first", stage: "standard", limits: { max_parallel: 4, max_candidates: 2, max_challenge_rounds: 1 } };
  const pending = pendingPolicy(key);
  const value = (name, fallback) => draft[name] ?? fallback;
  if (ro() || view.ok !== true || !Number.isSafeInteger(view.revision)) return A;
  return b2`<details><summary>${scope === "task" ? "\u8C03\u6574\u4EFB\u52A1\u7B56\u7565" : "\u8BBE\u7F6E\u65B0\u4EFB\u52A1\u7684\u9879\u76EE\u9ED8\u8BA4\u7B56\u7565"}</summary>
    <form data-form="delivery-policy" data-key=${key} data-scope=${scope} data-target=${target} data-revision=${view.revision}>
      <label>资源偏好 <select name="preset" .value=${value("preset", p4.preset)}>${modes.map(([id, label]) => b2`<option value=${id} ?selected=${value("preset", p4.preset) === id}>${label}</option>`)}</select></label>
      <label>工作阶段 <select name="stage" .value=${value("stage", p4.stage)}><option value="standard" ?selected=${value("stage", p4.stage) === "standard"}>正式交付</option><option value="prototype" ?selected=${value("stage", p4.stage) === "prototype"}>快速原型</option></select></label>
      ${[["max_parallel", "\u5206\u5DE5\u5E76\u53D1", 1, 32], ["max_candidates", "\u5B8C\u6574\u5019\u9009", 1, 8], ["max_challenge_rounds", "\u53EF\u9009\u6311\u6218\u8F6E\u6B21", 0, 8]].map(([name, label, min, max]) => b2`
        <label>${label} <input type="number" name=${name} min=${min} max=${max} step="1" .value=${String(value(name, p4.limits[name]))} required></label>`)}
      ${scope === "task" ? b2`<label><input type="checkbox" name="inherit" ?checked=${draft.inherit === true}>改为继承当前项目默认快照</label>` : A}
      <label>切换原因 <input name="reason" .value=${value("reason", "")} required></label>
      <label>逐字回填 ${target} <input name="confirm" .value=${value("confirm", "")} autocomplete="off" required></label>
      <div class="note">版本 ${view.revision}。新策略用于后续工作，费用需单独授权。</div>
      ${pending ? b2`<div class="warn">请求结果待对账：<code>${pending.body.operation_id}</code>。不自动重投，也不创建新操作绕过。</div><button type="button" data-act="policyrecheck" data-scope=${scope} data-target=${target}>只读核对原请求</button>` : b2`<button type="submit" class="primary" ?disabled=${state.busyActions.has(`policy:${key}`)}>保存策略</button>`}
    </form></details>`;
}
function renderDeliveryPolicy(task, view, projectView) {
  if (!view) return b2`<h3>交付策略</h3><div class="muted">当前服务未提供策略信息。</div>`;
  if (view.ok !== true) return b2`<h3>交付策略</h3><div class="bad">策略不可读：${view.code || "unknown"}，暂时无法调整。</div>`;
  const p4 = view.version?.policy;
  const inflight = view.inflight || { items: [], total: 0 };
  return b2`<section class="policy-panel"><h3>交付策略</h3>
    <div>${view.managed ? `${modes.find(([id]) => id === p4?.preset)?.[1] || p4?.preset} / ${p4?.stage === "prototype" ? "\u5FEB\u901F\u539F\u578B" : "\u6B63\u5F0F\u4EA4\u4ED8"} / v${view.revision}` : "\u672A\u914D\u7F6E\u4EFB\u52A1\u7B56\u7565"}</div>
    <div class=${view.execution_enforced ? "note" : "warn"}>${view.execution_enforced ? "\u540E\u7EED\u53D7\u7BA1\u8F93\u5165\u5DF2\u63A5\u8BB8\u53EF\u5B88\u536B\uFF1B\u4E0D\u662F\u5168\u94FE\u8D39\u7528\u4FDD\u8BC1" : "\u7B56\u7565\u8349\u6848 \xB7 \u8FD0\u884C\u65F6\u5B88\u536B\u5C1A\u672A\u542F\u7528"}</div>
    <div class="note">来源：${view.version?.source ? `\u9879\u76EE\u9ED8\u8BA4 v${view.version.source.revision}` : "\u4EFB\u52A1\u663E\u5F0F\u7B56\u7565\u6216\u672A\u914D\u7F6E"}。独立资源授权 v${view.authorization?.revision ?? 0}；授权明细/撤销走独立 operator 入口。</div>
    <div class="note">共用协调者费用独立。任务仍按既定标准验收。</div>
    ${view.routing_context ? b2`<details><summary>模型路线与上下文记录</summary>
      <div>${view.routing_context.continuity_reason}</div><div>缺失/过期是 unknown，不是零开销或缓存命中，不保证换路更快。</div>
      <ul>${Object.entries(view.routing_context.observations || {}).map(([name, item]) => b2`<li>${name}：${item.state} / ${item.value == null ? "unknown" : String(item.value)} / 来源 ${item.source || "unknown"} / ${ts(item.observed_at)}</li>`)}</ul></details>` : A}
    ${inflight.total ? b2`<div class="warn">仍有 ${inflight.total} 项可能在途（${inflight.truncated ? "\u5217\u8868\u622A\u65AD" : "\u542B\u539FID\u672A\u77E5"}，不承诺撤回/退费）：</div><ul>${(inflight.items || []).map((item) => b2`<li>${item.attempt_id || item.input_id} / ${item.message_id} / v${item.policy_revision} / ${item.model} / ${item.cost} / ${ts(item.issued_at)}${item.older_policy ? " \xB7 \u4F7F\u7528\u65E7\u7B56\u7565" : ""}</li>`)}</ul>` : b2`<div class="muted">当前无已记录在途许可；这不证明未纳管路线或内部调用已停止。</div>`}
    ${policyForm(view, "task", task.id)}
    ${projectView ? policyForm(projectView, "project", task.project) : A}
  </section>`;
}

// src/views/prototype.js
var emptyPlan = () => ({
  question: "\u672C\u8F6E\u8981\u9A8C\u8BC1\u7684\u95EE\u9898",
  entry: "\u672C\u8F6E\u4F53\u9A8C\u5165\u53E3",
  startup: "\u9694\u79BB\u542F\u52A8\u65B9\u6CD5\uFF08\u4EC5\u5C55\u793A\uFF0C\u4E0D\u6267\u884C\uFF09",
  checks: ["\u9700\u8981\u6CE8\u518C\u5E76\u5B9E\u9645\u8FD0\u884C\u7684\u68C0\u67E5"],
  paths: [{ id: "main", features: ["pending"] }],
  features: [{ id: "pending", title: "\u5F85\u5B9E\u73B0\u83DC\u5355", state: "placeholder", source: "\u672A\u63A5\u540E\u7AEF\uFF0C\u96F6\u4E1A\u52A1\u526F\u4F5C\u7528", gap_ids: ["gap-1"] }],
  gaps: [{
    id: "gap-1",
    kind: "tradeoff",
    state: "open",
    risk: "ordinary",
    expected: "\u771F\u5B9E\u4E1A\u52A1\u63A5\u5165",
    current: "\u5360\u4F4D",
    reason: "\u672C\u8F6E\u5148\u9A8C\u8BC1\u4EA4\u4E92",
    impact: "\u4E0D\u53EF\u6B63\u5F0F\u4F7F\u7528",
    owner: "operator",
    next_action: "\u5B9E\u73B0\u540E\u6CE8\u518C\u8054\u8C03\u68C0\u67E5",
    review_at: "\u5F85\u5B9A\u56DE\u8BBF\u65F6\u95F4",
    refs: ["\u672CTask"],
    followup_task: null,
    evidence: null
  }]
});
var labels2 = { real: "\u771F\u5B9E\u63A5\u5165", mock: "\u6A21\u62DF\u6570\u636E", placeholder: "\u5C1A\u672A\u5B9E\u73B0", blocked: "\u6682\u4E0D\u53EF\u7528" };
function renderPrototype(task, view) {
  if (!view) return A;
  if (view.ok !== true) return b2`<section><h3>原型范围与缺口</h3><div class="bad">无法读取原型记录 · ${view.code || "\u672A\u77E5\u9519\u8BEF"}。恢复读取后可编辑。</div></section>`;
  const plan = view.current?.plan, key = `prototype:${task.id}`;
  const draft = state.prototypeDrafts.get(task.id) || {};
  const pending = pendingPolicy(key);
  const selected = plan?.features.find((f3) => f3.id === state.prototypeSelected.get(task.id));
  return b2`<section class="policy-panel"><h3>原型范围与缺口 / v${view.revision}</h3>
    <div class="warn">此处记录原型计划。检查结果和修复证据见任务验收。</div>
    ${plan ? b2`<dl class="kv"><dt>验证问题</dt><dd>${plan.question}</dd><dt>体验入口</dt><dd>${plan.entry}</dd>
      <dt>启动说明</dt><dd><code>${plan.startup}</code></dd><dt>计划运行的检查</dt><dd>${plan.checks.join("\uFF1B")}</dd>
      <dt>代表路径</dt><dd>${plan.paths.map((p4) => b2`<div>${p4.id}：${p4.features.join(" \u2192 ")}</div>`)}</dd></dl>
      <div class="op-panel">${plan.features.map((f3) => b2`<button data-act="prototypefeature" data-task=${task.id} data-feature=${f3.id}>${f3.title} · ${labels2[f3.state] || "unknown"}</button>`)}</div>
      ${selected ? b2`<div role="status" data-prototype-detail class="attn rank-mid"><strong>${selected.title}：${labels2[selected.state]}</strong>
        <div>数据来源与影响 · ${selected.source}。这里只展示计划。</div><div>待解决事项 · ${selected.gap_ids.join("\u3001") || "\u65E0\u5EF6\u671F\u5173\u8054\uFF1B\u771F\u5B9E\u9A8C\u6536\u4ECD\u8D70collect/verify"}</div></div>` : A}
      <div>未闭环 ${view.unresolved?.length ?? "unknown"} 项。</div>
      ${plan.gaps.map((g2) => b2`<details><summary>${g2.id} / ${g2.kind} / ${g2.state} / 风险 ${g2.risk}</summary>
        <dl class="kv"><dt>期望/现状</dt><dd>${g2.expected} / ${g2.current}</dd><dt>原因/影响</dt><dd>${g2.reason} / ${g2.impact}</dd>
          <dt>责任/下一动作</dt><dd>${g2.owner} / ${g2.next_action}</dd><dt>回访</dt><dd>${g2.review_at}</dd><dt>引用</dt><dd>${g2.refs.join("\uFF1B")}</dd>
          <dt>后续Task/修复证据</dt><dd>${g2.followup_task || "\u539FTask"} / ${g2.evidence ? `${g2.evidence.attempt_id}@${g2.evidence.verified_at}` : "\u672A\u4FEE\u590D\uFF0C\u65E0\u6B63\u5F0F\u8BC1\u636E"}</dd></dl></details>`)}
      <details><summary>保留的原Task承诺</summary><pre>${JSON.stringify(view.commitment, null, 2)}</pre></details>` : b2`<div class="muted">尚无原型计划。</div>`}
    ${!ro() && !view.read_only ? b2`<details><summary>编辑原型计划</summary>
      <form data-form="prototype" data-task=${task.id} data-revision=${view.revision}>
        <label>计划JSON <textarea name="plan" rows="12" .value=${draft.plan ?? JSON.stringify(plan || emptyPlan(), null, 2)}></textarea></label>
        <label>更新原因 <input name="reason" .value=${draft.reason ?? ""}></label><label>逐字确认Task <input name="confirm" placeholder=${task.id} .value=${draft.confirm ?? ""}></label>
        ${pending ? b2`<div class="warn">原操作待对账：${pending.body.operation_id}；不换ID重投。</div>
          <button type="button" data-act="prototyperecheck" data-task=${task.id}>只读核对原记录</button>` : b2`<button type="submit" ?disabled=${state.busyActions.has(key)}>保存原型计划</button>`}
      </form></details>` : A}</section>`;
}

// src/views/task.js
function renderTask(id, data = {}) {
  const t5 = data.task || {};
  const attempts = data.attempts || [];
  const cur = attempts.find((a3) => a3.id === t5.current_attempt_id);
  const ver = cur && cur.verification ? cur.verification : {};
  const eff = t5.effective_coordinator;
  const src = t5.coordinator_source;
  const coordinator = eff ? b2`<a href=${`#/coordinator/${encodeURIComponent(eff)}`}>${state.coordMap[eff]?.title || eff}</a>` : b2`<span class="warn">尚未分配</span>`;
  const acts = [];
  if (!ro()) {
    if (t5.status === "paused") acts.push(b2`<button data-act="taskresume" data-id=${t5.id}>恢复任务</button>`);
    else if (!["accepted", "cancelled"].includes(t5.status)) acts.push(b2`<button data-act="taskpause" data-id=${t5.id}>暂停任务</button>`);
    if (!["accepted", "cancelled"].includes(t5.status)) acts.push(b2`<button class="danger" data-act="taskcancel" data-id=${t5.id} data-title=${t5.title || t5.id}>取消任务</button>`);
    if (cur && ["result_pending", "result_received", "result_collected", "result_missing"].includes(cur.status)) {
      acts.push(b2`<button class="primary" data-act="verify" data-id=${cur.id} data-pass="1">通过验收</button>`);
      acts.push(b2`<button data-act="verify" data-id=${cur.id} data-pass="0">验收不通过</button>`);
    }
    if (cur) acts.push(b2`<button data-act="collect" data-id=${cur.id}>收集结果</button>`);
  }
  const canRework = !ro() && cur && !["paused", "cancelled", "accepted"].includes(t5.status) && [
    "terminal_succeeded",
    "terminal_failed",
    "terminal_interrupted",
    "result_collected",
    "result_missing",
    "verification_failed",
    "rejected"
  ].includes(cur.status);
  const historyNote = data.attempts_truncated ? `\uFF0C\u4EC5\u6700\u8FD1 ${attempts.length} \u6761` : "";
  return b2`<div class="breadcrumb"><a href="#/">总览</a> <span class="sep">/</span> <a href="#/tasks">全局任务</a> <span class="sep">/</span> <span>${t5.title || t5.id}</span></div>
    <div class="row"><h2>${t5.title || t5.id}</h2>${badge(t5.status)}<span class="spacer"></span>${copyButton(t5.id)}</div>
    <div class="task-context"><span>${t5.project || "\u9879\u76EE\u672A\u77E5"}</span><span>调度者 · ${coordinator}</span></div>
    ${t5.blocked_reason ? b2`<div class="attn rank-mid"><strong>受阻原因</strong>${t5.blocked_reason}</div>` : A}
    <div class="task-brief">
      <section><h3>目标</h3><pre>${t5.objective || "\u5C1A\u672A\u586B\u5199"}</pre></section>
      <section><h3>验收标准</h3><pre>${Array.isArray(t5.acceptance) ? t5.acceptance.join("\n") || "\u5C1A\u672A\u586B\u5199" : t5.acceptance || "\u5C1A\u672A\u586B\u5199"}</pre></section>
    </div>
    <details class="scope"><summary>任务信息与时间</summary><dl class="kv">
      <dt>任务 ID</dt><dd><code class="mono">${t5.id}</code></dd>
      ${eff ? b2`<dt>调度会话</dt><dd><code class="mono">${eff}</code> · ${src === "lease" ? "\u7531\u9879\u76EE\u8C03\u5EA6\u8005\u8D1F\u8D23" : "\u4EFB\u52A1\u5355\u72EC\u6307\u5B9A"}</dd>` : A}
      <dt>创建时间</dt><dd>${ts(t5.created_at)}</dd>
      <dt>最近更新</dt><dd>${ts(t5.updated_at)}</dd>
      <dt>验收 / 关闭时间</dt><dd>${ts(t5.accepted_at)} / ${ts(t5.closed_at)}</dd>
      <dt>期限 / 下次检查</dt><dd>${ts(t5.deadline_at)} / ${ts(t5.next_check_at)}</dd>
    </dl></details>
    <h3>当前尝试${cur ? b2` · 第 ${cur.generation} 次` : A}</h3>
    ${cur ? b2`<dl class="kv">
      <dt>尝试 ID</dt><dd><a href=${`#/attempt/${encodeURIComponent(cur.id)}`}><code class="mono">${cur.id}</code></a></dd>
      <dt>状态</dt><dd>${badge(cur.status)} · 派单 ${badge(cur.dispatch_state || "-")}</dd>
      <dt>执行会话</dt><dd><code class="mono">${cur.session_id || "-"}</code></dd>
      <dt>创建/最近进展</dt><dd>${ts(cur.created_at)} / ${ts(cur.last_progress_at)}</dd>
      ${ver.result ? b2`<dt>验收</dt><dd>${badge(ver.result)} · ${ver.classification || "-"} · ${ts(ver.verified_at)} · 验收人 ${ver.actor || "-"}</dd>` : A}
      ${ver.missing && ver.missing.length ? b2`<dt>未满足</dt><dd class="bad">${ver.missing.join("\u3001")}</dd>` : A}
    </dl><div class="note">执行与证据详情见 <a href=${`#/attempt/${encodeURIComponent(cur.id)}`}>尝试详情</a>。</div>` : b2`<div class="empty">当前无尝试。</div>`}
    <h3>任务操作${ro() ? " \xB7 \u53EA\u8BFB" : ""}</h3>
    <div class="op-panel">${acts.length ? acts : b2`<span class="muted">当前状态无可执行操作${ro() ? " \xB7 \u53EA\u8BFB" : ""}</span>`}</div>
    <div id="taskFormHost">${renderVerifyForm("task:" + id)}</div>
    ${canRework ? renderReworkForm(t5.id) : A}
    ${renderDeliveryPolicy(t5, data.delivery_policy, data.project_delivery_policy)}
    ${renderPrototype(t5, data.prototype)}
    <h3>历史尝试 · ${data.attempts_full_count ?? attempts.length} 次${historyNote}</h3>
    <div class="tablewrap"><table><thead><tr><th>尝试 ID</th><th>次数</th><th>状态</th><th>结果路径</th><th>验收</th></tr></thead><tbody>
      ${attempts.length ? c4(attempts, (a3) => a3.id, (a3) => b2`<tr><td><a href=${`#/attempt/${encodeURIComponent(a3.id)}`}><code class="mono">${a3.id}</code></a></td><td>${a3.generation}</td><td>${badge(a3.status)}</td><td class="mono">${a3.result_path || "-"}</td><td>${a3.verification && a3.verification.result ? b2`${badge(a3.verification.result)} ${a3.verification.classification || ""}` : b2`<span class="muted">-</span>`}</td></tr>`) : b2`<tr><td colspan="5" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>`;
}

// src/views/attempt.js
function renderAttempt(id, data = {}) {
  const a3 = data.attempt || {};
  const ver = a3.verification || {};
  const acts = [];
  if (!ro()) {
    if (["result_pending", "result_received", "result_collected", "result_missing"].includes(a3.status)) {
      acts.push(b2`<button class="primary" data-act="verify" data-id=${a3.id} data-pass="1">通过验收</button>`);
      acts.push(b2`<button data-act="verify" data-id=${a3.id} data-pass="0">验收不通过</button>`);
    }
    if (a3.id) acts.push(b2`<button data-act="collect" data-id=${a3.id}>收集结果</button>`);
  }
  const dels = data.deliverables || [];
  const verdict = ver && ver.result ? b2`<div class="attn ${ver.result === "pass" ? "" : "rank-high"}" style=${ver.result === "pass" ? "border-color:#2ea04388" : "border-color:#f8514955"}><span>${badge(ver.result)} ${ver.classification || ""} · ${ts(ver.verified_at)} · 验收人 ${ver.actor || "-"}${ver.force_reason ? b2` · 强制通过原因 ${ver.force_reason}` : A}${ver.reason ? b2` · 原因：${ver.reason}` : A}</span>${ver.missing && ver.missing.length ? b2`<div class="note bad">未满足：${ver.missing.join("\u3001")}${(ver.unmet || []).length ? `\uFF1B\u672A\u6EE1\u8DB3\u9879 ${JSON.stringify(ver.unmet)}` : ""}</div>` : A}${ver.evidence && ver.evidence.length ? b2`<div class="note">证据：<code class="mono">${ver.evidence.join("\uFF1B")}</code></div>` : A}</div>` : b2`<div class="note">尚无验收结论。</div>`;
  return b2`<div class="breadcrumb"><a href="#/">总览</a> <span class="sep">/</span> <a href=${`#/task/${encodeURIComponent(a3.task_id || "")}`}>任务 ${a3.task_id || ""}</a> <span class="sep">/</span> <span>${a3.id}</span></div>
    <div class="row"><h2>尝试 ${a3.id}</h2>${badge(a3.status)}<span class="spacer"></span>${copyButton(a3.id)}</div>
    <dl class="kv">
      <dt>任务</dt><dd><a href=${`#/task/${encodeURIComponent(a3.task_id || "")}`}><code class="mono">${a3.task_id || "-"}</code></a> · ${data.task ? data.task.title : ""} ${data.task ? badge(data.task.status) : A}</dd>
      <dt>代次 / 派单</dt><dd>第 ${a3.generation} 次 · ${badge(a3.dispatch_state || "-")}${a3.dispatch_msg_id ? b2` <code class="mono">${a3.dispatch_msg_id}</code>` : A}</dd>
      <dt>执行会话</dt><dd><code class="mono">${a3.session_id || "-"}</code></dd>
      <dt>目录</dt><dd><code class="mono">${a3.directory || "-"}</code></dd>
      <dt>创建 / 派单 / 启动</dt><dd>${ts(a3.created_at)} / ${ts(a3.dispatched_at)} / ${ts(a3.started_at)}</dd>
      <dt>最近进展 / 观察</dt><dd>${ts(a3.last_progress_at)} / ${ts(a3.last_observed_at)}</dd>
      <dt>停止请求 / 已确认 / 现场释放</dt><dd>${ts(a3.stop_requested_at)} / ${ts(a3.stop_confirmed_at)} / ${ts(a3.site_released_at)}</dd>
      <dt>结果收集</dt><dd>${ts(a3.result_collected_at)}</dd>
      <dt>终态</dt><dd>${ts(a3.terminal_at)} · ${a3.terminal_kind || "-"}${a3.terminal_reason ? b2`<div class="note">${a3.terminal_reason}</div>` : A}</dd>
    </dl>
    <h3>验收结论</h3>
    ${verdict}
    <h3>结果摘要</h3><pre class="log">${a3.result_summary || "-"}</pre>
    ${a3.result_path ? b2`<div class="note">结果路径：<code class="mono">${a3.result_path}</code></div>` : A}
    <h3>尝试操作${ro() ? " \xB7 \u53EA\u8BFB" : ""}</h3>
    <div class="op-panel" id="attOps">${acts.length ? acts : b2`<span class="muted">当前状态无可执行操作${ro() ? " \xB7 \u53EA\u8BFB" : ""}</span>`}</div>
    <div id="attemptFormHost">${renderVerifyForm("attempt:" + id)}</div>
    <h3>交付清单 · ${dels.length} 项</h3>
    <div class="tablewrap"><table><thead><tr><th>交付物</th><th>类型</th><th>状态</th><th>检查时间</th><th>证据</th></tr></thead><tbody>
      ${dels.length ? c4(dels, (d5) => `${d5.target || ""}:${d5.kind || ""}`, (d5) => b2`<tr><td class="mono">${d5.target || ""}</td><td>${d5.kind || ""}</td><td>${badge(d5.status || "-")}</td><td>${ts(d5.checked_at)}</td><td class="mono">${d5.evidence ? JSON.stringify(d5.evidence) : "-"}</td></tr>`) : b2`<tr><td colspan="5" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>`;
}

// src/views/tasks.js
function taskTable(list, { unbound = false, empty = "\u6682\u65E0\u4EFB\u52A1" } = {}) {
  return b2`<div class="tablewrap task-table"><table><thead><tr>
    <th>任务</th><th>状态</th>${unbound ? A : b2`<th>指定调度者</th>`}<th>${unbound ? "\u6700\u8FD1\u66F4\u65B0" : "\u6700\u8FD1\u8FDB\u5C55"}</th>
    </tr></thead><tbody>${list.length ? c4(list, (t5) => t5.id, (t5) => b2`<tr>
      <td><a class="task-title" href=${`#/task/${encodeURIComponent(t5.id)}`}>${t5.title || t5.id}</a>
        <div class="note">${t5.project} <code>${t5.id}</code></div></td>
      <td>${badge(t5.status)}${t5.blocked_reason ? b2`<div class="note">${t5.blocked_reason}</div>` : A}</td>
      ${unbound ? A : b2`<td>${t5.coordinator_session ? b2`<a href=${`#/coordinator/${encodeURIComponent(t5.coordinator_session)}`}>${state.coordMap[t5.coordinator_session]?.title || t5.coordinator_session}</a>` : "\u672A\u5355\u72EC\u6307\u5B9A"}</td>`}
      <td>${ts(unbound ? t5.updated_at : t5.last_progress_at)}</td>
    </tr>`) : b2`<tr><td colspan=${unbound ? 3 : 4}>${empty}</td></tr>`}</tbody></table></div>`;
}
function renderTasksView() {
  const tasks = state.snap?.tasks || [];
  const total = state.snap?.counts?.tasks;
  const q = (state.search || "").toLowerCase().trim();
  const list = q ? tasks.filter((t5) => [t5.id, t5.title, t5.project].some((v3) => v3?.toLowerCase().includes(q))) : tasks;
  return b2`<h2>全局任务</h2>
    <div class="row"><label class="search-field"><input type="text" id="taskSearch" value=${state.search}
      aria-label="搜索已加载的任务" placeholder="搜索任务标题、项目或 ID"></label>
      <span class="note">${q ? `\u627E\u5230 ${list.length} \u9879 \xB7 ` : ""}已加载 ${tasks.length} 项${total != null ? `\uFF0C\u5171 ${total} \u9879` : ""}</span></div>
    ${taskTable(list, { empty: q ? "\u6CA1\u6709\u5339\u914D\u7684\u4EFB\u52A1" : "\u6682\u65E0\u4EFB\u52A1" })}`;
}
function renderUnboundTasks() {
  const rows = state.unboundRows || [];
  return b2`<div class="breadcrumb"><a href="#/">总览</a> / 待分配任务</div>
    <h2>待分配任务</h2><p class="note">这些任务尚无调度者负责。已加载 ${rows.length} 项，共 ${state.unboundFull} 项。</p>
    ${taskTable(rows, { unbound: true, empty: "\u6682\u65E0\u5F85\u5206\u914D\u4EFB\u52A1" })}
    <div class="op-panel">${state.unboundTruncated ? b2`<button class="primary" data-act="unboundmore" ?disabled=${state.unboundLock}>继续加载</button>` : A}<a href="#/tasks">查看全局任务</a></div>`;
}

// src/views/diagnostics.js
var managedSession = (i8) => i8.session_id || i8.session || "-";
function notifSection(ns) {
  return b2`<section id="diag-notif" aria-label="通知与告警">
    <h2>通知</h2>
    <div class="row" style="margin-bottom:8px">
      <span class="muted" id="notifMeta">已列出 ${ns.length} 条通知</span>
      <span class="spacer"></span>
      <button id="ackCheckedBtn" data-act="ackchecked" class="primary" ?disabled=${ro()}>确认收到所选通知</button>
    </div>
    <div class="tablewrap"><table id="notifTable"><thead><tr><th>编号 / 类型</th><th>状态</th><th>目标会话</th><th>摘要</th><th>最近错误</th></tr></thead><tbody>
      ${ns.length ? c4(ns, (n4) => n4.id, (n4) => b2`<tr><td><label class="inline"><input type="checkbox" class="noteck" data-id=${n4.id} checked> <code class="mono">${n4.id}</code></label><div class="muted">${n4.kind}</div></td><td>${badge(n4.state)}</td><td><code class="mono">${n4.target_session || "-"}</code>${n4.task_id || n4.attempt_id ? b2`<div class="muted">${n4.task_id || ""} ${n4.attempt_id || ""}</div>` : A}</td><td>${n4.summary || ""}${n4.missing_deliverables && n4.missing_deliverables.length ? b2`<div class="bad">missing ${n4.missing_deliverables.join("\u3001")}</div>` : A}</td><td class="mono">${n4.last_error || "-"}</td></tr>`) : b2`<tr><td colspan="5" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>
    <div class="note">确认收到后，任务仍需单独验收。</div>
  </section>`;
}
function alertSection(alerts, title = "\u544A\u8B66") {
  return b2`<section id="diag-alerts" aria-label="告警">
    <h2>${title}</h2>
    <div class="row" style="margin-bottom:8px">
      <span class="muted" id="alertMeta">当前快照中有 ${alerts.length} 条，上限 50 条</span>
      <span class="spacer"></span>
      <button id="ackAlertsBtn" data-act="ackalerts" class="primary" ?disabled=${ro()}>确认所选告警</button>
    </div>
    <div class="tablewrap"><table id="alertTable"><thead><tr><th>ID</th><th>级别</th><th>标题</th><th>归属</th><th>详情</th></tr></thead><tbody>
      ${alerts.length ? c4(alerts, (a3) => a3.id, (a3) => b2`<tr><td><label class="inline"><input type="checkbox" class="alck" data-id=${a3.id} checked> <code class="mono">${a3.id}</code></label></td><td>${a3.severity || ""}</td><td>${badge(a3.kind)} ${a3.title || ""}</td><td>${a3.task_id || a3.attempt_id ? b2`<span class="muted">task ${a3.task_id || ""} attempt ${a3.attempt_id || ""}</span>` : b2`<span class="warn">未关联任务</span>`}</td><td><details><summary class="muted">查看原文</summary><pre>${a3.detail || ""}</pre></details></td></tr>`) : b2`<tr><td colspan="5" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>

  </section>`;
}
function gatesSection() {
  const coords = state.coordList.map((sid) => state.coordMap[sid]).filter(Boolean);
  return b2`<section id="diag-gates" aria-label="协调者解锁">
    <h2>恢复调度</h2>
    <div id="gatesRoot"><div class="tablewrap"><table><thead><tr><th>会话</th><th>调度限制</th><th></th></tr></thead><tbody>
      ${coords.length ? c4(coords, (c5) => c5.session, (c5) => {
    const ua = unlockActionFor(c5);
    return b2`<tr><td><a href=${`#/coordinator/${encodeURIComponent(c5.session)}`}><code class="mono">${c5.session}</code></a></td><td>${(c5.gates || []).length ? c5.gates.map(gateBadge) : b2`<span class="muted">无闸门</span>`}</td><td>${ua ? b2`<button class="danger" data-act="unlock" data-sid=${c5.session} data-project=${ua.project} data-allow-provider=${ua.allowProvider ? "1" : "0"} ?disabled=${ro()}>${ua.label}</button>` : b2`<span class="muted">无可执行操作</span>`}</td></tr>`;
  }) : b2`<tr><td colspan="3" class="muted">暂无记录</td></tr>`}
    </tbody></table></div></div>
    <div class="note">解除限制需要填写原因并输入 UNLOCK。遇到服务商限制时，恢复后会先重新探测。</div>
  </section>`;
}
function supSection() {
  return b2`<section id="diag-supervisor" aria-label="后台调度服务">
    <h2>后台调度服务</h2>
    <div class="op-panel" id="supRoot"><button class="primary" data-act="supstart" ?disabled=${ro()}>启动调度服务</button><button class="danger" data-act="supstop" ?disabled=${ro()}>停止调度服务</button></div>
    <div class="note">启动需输入 START，停止需输入 STOP。</div>
  </section>`;
}
function leasesSection(snap) {
  const leases = snap.leases || [];
  const back = snap.provider_backoffs || {};
  const gates = snap.target_gates || [];
  return b2`<section id="diag-leases" aria-label="租约与退避">
    <h2>项目租约与服务商限制</h2>
    <details id="meta"><summary>查看原始记录</summary>
      <h3>Leases</h3><pre>${JSON.stringify(leases, null, 1)}</pre>
      <h3>Provider backoff</h3><pre>${JSON.stringify(back, null, 1)}</pre>
      <h3>Target gates</h3><pre>${JSON.stringify(gates, null, 1)}</pre>
    </details>
  </section>`;
}
function sendSection() {
  const r4 = ro();
  return b2`<section id="diag-sup-input" aria-label="发送受管输入">
    <h2>发送会话指令</h2>
    <div id="sendFormHost"><form id="sendForm" data-form="send" class="row">
      <label>会话 ID<input type="text" name="session" placeholder="ses_…" required ?disabled=${r4}></label>
      <label>处理方式<select name="intent" ?disabled=${r4}><option value="execute">执行</option><option value="park">暂存</option><option value="record">仅记录</option></select></label>
      <label>文本<input type="text" name="text" required ?disabled=${r4}></label>
      <label>负责人<input type="text" name="owner" required ?disabled=${r4}></label>
      <label>请求 ID<input type="text" name="request_id" required ?disabled=${r4}></label>
      <label>项目<input type="text" name="project" ?disabled=${r4}></label>
      <label>任务 ID<input type="text" name="task" ?disabled=${r4}></label>
      <label>尝试 ID<input type="text" name="attempt" ?disabled=${r4}></label>
      <label>目标会话<input type="text" name="target_session" ?disabled=${r4}></label>
      <button class="primary" type="submit" ?disabled=${r4}>发送</button>
    </form></div>
    <div class="note">可在指令记录中查看投递和执行进度。</div>
  </section>`;
}
function managedSection(inputs, watches) {
  const r4 = ro();
  return b2`<section id="diag-managed" aria-label="受管输入与白名单">
    <h2>指令记录与会话观察</h2>
    <div class="muted" id="managedMeta">已读取 ${inputs.length} 条指令 · ${watches.length} 个观察会话</div>
    <div class="tablewrap"><table><thead><tr><th>指令 / 消息</th><th>会话</th><th>负责人</th><th>处理方式</th><th>受理</th><th>投递</th><th>执行</th><th>处理确认</th><th>年龄 / 下一动作</th><th>操作</th></tr></thead><tbody id="managedBody">
      ${inputs.length ? c4(inputs, (i8) => i8.input_id || i8.message_id || JSON.stringify(i8), (i8) => b2`<tr>
        <td><code class="mono">${i8.input_id || "-"}</code><div class="muted">${i8.message_id || "-"}</div></td>
        <td><code class="mono">${managedSession(i8)}</code></td>
        <td>${i8.owner || "-"}</td>
        <td>${i8.intent || "-"}</td>
        <td>${managedBadge(i8.admission)}</td>
        <td>${managedBadge(i8.delivery)}</td>
        <td>${managedBadge(i8.execution)}</td>
        <td>${managedBadge(i8.business_ack)}</td>
        <td>${fmtAge(i8.pending_age_ms)}<div class="muted">next ${i8.next_action || "-"}</div></td>
        <td>${r4 ? "-" : b2`${["flush", "cancel"].map((a3) => b2`<button data-managed=${a3} data-id=${i8.input_id} data-rev=${i8.revision == null ? "" : i8.revision}>${a3 === "flush" ? "\u7ACB\u5373\u6295\u9012" : "\u53D6\u6D88"}</button>`)}${i8.business_ack !== "acknowledged" ? b2`<button class="primary" data-managed="ack" data-id=${i8.input_id} data-rev=${i8.revision == null ? "" : i8.revision}>确认处理</button>` : A}`}</td>
      </tr>`) : b2`<tr><td colspan="10" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>
    <div class="muted" style="margin-top:8px">已登记的观察会话</div>
    <div class="tablewrap"><table><thead><tr><th>会话</th><th>负责人</th><th>项目</th><th>状态</th><th>最长等待时间</th><th>版本 / 待处理</th><th>最近观察</th><th>下一动作</th></tr></thead><tbody id="watchBody">
      ${watches.length ? c4(watches, (w2) => w2.session_id || w2.session || JSON.stringify(w2), (w2) => b2`<tr>
        <td><code class="mono">${w2.session_id || w2.session || "-"}</code></td>
        <td>${w2.owner || "-"}</td>
        <td>${w2.project || "-"}</td>
        <td>${watchEnabledBadge(w2.enabled)}</td>
        <td>${w2.max_pending_age_ms == null ? "-" : w2.max_pending_age_ms + " ms"}</td>
        <td>${w2.revision == null ? "-" : w2.revision}${w2.pending_count != null ? b2`<div class="muted">pending ${w2.pending_count}</div>` : A}</td>
        <td>${watchObservation(w2)}</td>
        <td>${w2.next_action || "-"}</td>
      </tr>`) : b2`<tr><td colspan="8" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>
    <div class="note">操作前需输入对应的指令 ID。</div>
  </section>`;
}
function respSection(resp, title = "\u5F85\u8DDF\u8FDB\u4E8B\u9879") {
  return b2`<section>
    <h2>${title}</h2>
    <div class="muted" id="respMeta">当前列出 ${resp.length} 条，从最多 200 条未关闭事项中读取。</div>
    <div class="tablewrap"><table><thead><tr><th>ID / 状态</th><th>任务 / 尝试</th><th>负责人 / 目标</th><th>等待条件</th><th>下一动作</th><th>期限 / 预算</th></tr></thead><tbody id="respBody">
      ${resp.length ? c4(resp, (x2) => x2.id || JSON.stringify(x2), (r4) => b2`<tr>
        <td><code class="mono">${r4.id || "-"}</code><div>${managedBadge(r4.state)}</div></td>
        <td>${r4.task_id || "-"}<div class="muted">${r4.attempt_id || ""} g=${r4.generation == null ? "-" : r4.generation} rv=${r4.result_version || "-"}</div></td>
        <td>${r4.owner || "-"} → ${r4.target_session || "-"}</td>
        <td>${r4.wait_kind || "-"}<div class="muted">${r4.wait_condition || "-"}</div></td>
        <td>${r4.next_action || "-"}<div class="muted">next_check ${ts(r4.next_check_at)}</div></td>
        <td>${r4.deadline == null ? "-" : ts(r4.deadline)}<div class="muted">budget ${r4.budget_used == null ? "-" : r4.budget_used}/${r4.max_budget == null ? "-" : r4.max_budget} · rev ${r4.revision == null ? "-" : r4.revision}</div></td>
      </tr>`) : b2`<tr><td colspan="6" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>

  </section>`;
}
function auditSection() {
  const audit = (state.audit || []).map((a3, index) => ({ ...a3, _i: index }));
  const auditKey = (a3) => a3.id != null ? String(a3.id) : `${a3._i}|${a3.at || 0}|${a3.action || ""}|${a3.entity || ""}|${a3.entity_id || ""}|${a3.from_state || ""}|${a3.to_state || ""}|${a3.actor || ""}`;
  return b2`<section>
    <h2>最近审计</h2>
    <div class="tablewrap"><table><thead><tr><th>时间</th><th>操作者</th><th>操作</th><th>对象</th><th>状态变化</th></tr></thead><tbody id="auditBody">
      ${audit.length ? c4(audit, auditKey, (a3) => b2`<tr><td>${ts(a3.at)}</td><td>${a3.actor || "-"}</td><td>${a3.action || "-"}</td><td>${a3.entity || ""} ${a3.entity_id || ""}</td><td>${a3.from_state || ""} → ${a3.to_state || ""}</td></tr>`) : b2`<tr><td colspan="5" class="muted">暂无记录</td></tr>`}
    </tbody></table></div>
  </section>`;
}
function bizHealthSection() {
  const h5 = state.health;
  const body = h5?.body;
  const tasks = body?.tasks || [];
  const proc = body?.process || {};
  const runtime = body?.runtime || {};
  const procLine = proc.healthy == null ? b2`<span class="muted">进程健康未知</span>` : proc.healthy ? b2`<span class="ok">进程正常</span> ${proc.connected != null ? `connected=${proc.connected}` : ""}${proc.heartbeat_age_s != null ? b2` · 心跳 ${proc.heartbeat_age_s}s 前` : ""}` : b2`<span class="bad">进程异常</span>`;
  const scopeNote = body?.coverage ? body.coverage.complete ? "\u8986\u76D6\u5B8C\u6574" : `\u90E8\u5206\u8986\u76D6 \xB7 \u5DF2\u663E\u793A ${tasks.length} \u9879` : "\u8986\u76D6\u672A\u77E5";
  const trow = (t5) => {
    const c5 = t5.control || {};
    const resp = (t5.responsibilities || [])[0] || {};
    const bits = [
      c5.project_intent ? `\u610F\u56FE ${c5.project_intent}` : null,
      c5.turn_held ? `hold${c5.hold?.reason ? " " + c5.hold.reason : ""}${c5.hold?.held_at ? "\uFF08" + ts(c5.hold.held_at) + "\uFF09" : ""}` : null,
      c5.user_turn_active ? "\u56DE\u5408\u4E2D" : null,
      c5.provider_gate ? `provider ${c5.provider_gate}` : null
    ].filter(Boolean).join(" \xB7 ");
    const ver = c5.project_revision != null ? `rev ${c5.project_revision}` : "";
    const ep = c5.epoch != null ? `epoch ${c5.epoch}` : "";
    return b2`<tr>
      <td><a href=${`#/task/${encodeURIComponent(t5.task_id || "")}`}><code class="mono">${t5.task_id || "-"}</code></a><div class="muted">${t5.project || ""}</div></td>
      <td>${badge(t5.state)}${t5.process_healthy != null ? b2`<div class="muted">proc ${t5.process_healthy ? "ok" : "down"}</div>` : A}</td>
      <td class="mono">${t5.owner || "-"}</td>
      <td>${t5.next_action || "-"}</td>
      <td>${resp.deadline != null ? ts(resp.deadline) : "-"}<div class="muted">next ${resp.next_check_at != null ? ts(resp.next_check_at) : "-"}</div></td>
      <td>${resp.budget_used != null ? `${resp.budget_used}/${resp.max_budget ?? "-"}` : "-"}</td>
      <td>${t5.evidence_coverage === true ? b2`<span class="ok">完整</span>` : t5.evidence_coverage === false ? b2`<span class="warn">不足</span>` : b2`<span class="muted">-</span>`}</td>
      <td>${bits || b2`<span class="muted">无控制记录</span>`}${ver || ep ? b2`<div class="muted">${ver}${ver && ep ? " \xB7 " : ""}${ep}</div>` : A}</td>
    </tr>`;
  };
  const runtimeBits = [];
  if (runtime.release_id) runtimeBits.push(b2`release ${runtime.release_id}`);
  if (runtime.manifest_verified === true) runtimeBits.push(b2`<span class="ok">manifest 校验通过</span>`);
  else if (runtime.source) runtimeBits.push(b2`<span class="muted">${runtime.source}</span>`);
  if (runtime.package_version) runtimeBits.push(b2`包版本 ${runtime.package_version}`);
  return b2`<section id="diag-bizhealth" aria-label="业务健康">
    <h2>业务健康</h2>
    <div class="note">任务完成以验收结论为准。 ${scopeNote}${state.healthErr ? b2` <span class="bad">刷新失败，保留旧数据</span>` : A}</div>
    <div class="note">${procLine}</div>
    <div class="tablewrap"><table><thead><tr><th>任务</th><th>状态</th><th>负责人</th><th>下一动作</th><th>期限 / 下次检查</th><th>预算</th><th>证据</th><th>调度限制</th></tr></thead><tbody>
      ${tasks.length ? c4(tasks, (t5) => t5.task_id || JSON.stringify(t5), trow) : b2`<tr><td colspan="8" class="muted">${h5 ? "\u5F53\u524D\u65E0\u672A\u5B8C\u6210\u4E1A\u52A1\u5065\u5EB7\u8BB0\u5F55\u3002" : "\u5065\u5EB7\u6570\u636E\u672A\u52A0\u8F7D\u3002"}</td></tr>`}
    </tbody></table></div>
    <div class="note" id="runtimeIdentity">运行版本 · ${runtimeBits.length ? runtimeBits : b2`未知`}${runtime.email_default_enabled === false ? " \xB7 \u90AE\u4EF6\u9ED8\u8BA4\u5173\u95ED" : ""}</div>
  </section>`;
}
function incidentsSection() {
  const items = state.incidents || [];
  const st = state.incidentStatus || {};
  const coverage = st.coverage || {};
  const chTxt = st.channel === "not-configured" ? "\u672A\u914D\u7F6E" : st.channel || "-";
  return b2`<section id="diag-incidents" aria-label="本地人工待办">
    <h2>本地人工待办</h2>
    <div class="muted" id="incidentMeta">已加载 ${items.length} / 共 ${st.total ?? "\u672A\u77E5"} 条 · ${coverage.scope_complete === true ? "\u8986\u76D6\u5B8C\u6574" : "\u90E8\u5206\u8986\u76D6\u6216\u672A\u77E5"} · 提醒渠道 ${chTxt} · 收件人 ${(st.recipient_refs || []).join(",") || "\u65E0"}${st.email_configured != null ? b2` · 邮件${st.email_configured ? "\u5DF2\u914D\u7F6E" : "\u672A\u914D\u7F6E"}` : A}</div>
    ${coverage.invalid_records || coverage.unattributed_records ? b2`<div class="warn">存在损坏或不可归属记录，不能据空列表认定无人工待办。</div>` : A}
    ${coverage.next_after_id ? b2`<div class="note">后页游标：<code>${coverage.next_after_id}</code> · 可使用 incidents --after-id 查看后续记录</div>` : A}
    <div class="tablewrap"><table><thead><tr><th>ID / 类型</th><th>状态</th><th>负责人 / 任务</th><th>下一动作</th><th>期限 / 首次</th><th>通道</th></tr></thead><tbody>
      ${items.length ? c4(items, (i8) => i8.incident_id || JSON.stringify(i8), (it) => b2`<tr>
        <td><code class="mono">${it.incident_id || "-"}</code><div class="muted">${it.incident_type || "-"} · ${it.source_kind || ""}${it.escalation_level ? ` \xB7 esc ${it.escalation_level}` : ""}</div></td>
        <td>${badge(it.state)}</td><td>${it.owner || "-"} → ${it.task_id || "-"}</td>
        <td>${it.next_action || "-"}</td>
        <td>${it.deadline ? ts(it.deadline) : ts(it.first_seen_at)}</td>
        <td>${it.channel || "-"}</td></tr>`) : b2`<tr><td colspan="6" class="muted">${coverage.scope_complete === true ? "\u5F53\u524D\u8303\u56F4\u6CA1\u6709\u767B\u8BB0\u7684\u5F85\u529E" : "\u5F53\u524D\u9875\u65E0\u53EF\u8BFB\u8BB0\u5F55\uFF0C\u5B8C\u6574\u8303\u56F4\u5C1A\u672A\u8BC1\u660E"}</td></tr>`}
    </tbody></table></div>
  </section>`;
}
function renderDiagnostics(section) {
  const snap = state.snap || {};
  const ns = snap.notifications || [];
  const alerts = snap.alerts || [];
  const inputs = state.inputs || [];
  const watches = state.watches || [];
  const resp = state.resp || [];
  if (section) {
    const showAlerts = section === "alerts";
    const title = showAlerts ? "\u672A\u5173\u8054\u4EFB\u52A1\u7684\u544A\u8B66" : "\u5176\u4ED6\u8D1F\u8D23\u4EBA\u5F85\u529E";
    return b2`<div class="breadcrumb"><a href="#/">总览</a> / <a href="#/diagnostics">系统诊断</a></div>
      ${showAlerts ? alertSection(alerts.filter((a3) => !a3.task_id && !a3.attempt_id), title) : state.coordMeta ? respSection(resp.filter((r4) => !state.coordMap[r4.owner]), title) : b2`<h2>${title}</h2><p>无法读取会话归属。</p>`}`;
  }
  return b2`<h2>系统诊断</h2>
    ${notifSection(ns)}
    ${alertSection(alerts)}
    ${respSection(resp)}
    ${incidentsSection()}
    ${gatesSection()}
    ${supSection()}
    ${leasesSection(snap)}
    ${sendSection()}
    ${managedSection(inputs, watches)}
    ${bizHealthSection()}
    ${auditSection()}`;
}

// src/router.js
var generation = 0;
function parseHash() {
  const parts = location.hash.replace(/^#\/?/, "").split("/").filter(Boolean);
  try {
    if (parts[0] === "tasks") return { view: "tasks", unbound: parts[1] === "unbound" };
    if (parts[0] === "diagnostics") {
      const section = ["alerts", "responsibilities"].includes(parts[1]) ? parts[1] : "";
      return { view: "diagnostics", section };
    }
    if (parts[0] === "coordinator") return { view: "coordinator", sid: decodeURIComponent(parts[1] || "") };
    if (["task", "attempt"].includes(parts[0])) return { view: parts[0], id: decodeURIComponent(parts[1] || "") };
  } catch {
  }
  return { view: "overview" };
}
var routeKey = (route = parseHash()) => `${route.view}:${route.sid || route.id || route.section || (route.unbound ? "unbound" : "")}`;
var needDetail = (route) => ["coordinator", "task", "attempt"].includes(route.view) || !!route.unbound;
var routeGeneration = () => generation;
function routeIsCurrent(key, version) {
  return routeKey() === key && generation === version;
}
function navigate() {
  if (state.currentKey) state.scroll[state.currentKey] = window.scrollY;
  generation += 1;
  state.currentKey = routeKey();
  state.detailErr = false;
  publish();
}
function restoreNavigation(key) {
  requestAnimationFrame(() => {
    if (!key || routeKey() !== key) return;
    const main = document.getElementById("main");
    if (!main) return;
    window.scrollTo(0, state.scroll[key] || 0);
    main.focus({ preventScroll: true });
  });
}

// src/api.js
var ApiError = class extends Error {
  constructor(message, status = 0, code = "NETWORK") {
    super(message);
    this.status = status;
    this.code = code;
  }
};
var isNotFound = (error) => !!error && error instanceof ApiError && (error.status === 404 || /NOT_FOUND/.test(String(error.code || "")));
async function api(path, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), options.method === "POST" ? 12e4 : 12e3);
  try {
    const response = await fetch(path, { credentials: "same-origin", ...options, signal: controller.signal });
    let body = {};
    try {
      body = await response.json();
    } catch {
      throw new ApiError("\u54CD\u5E94\u4E0D\u662F\u6709\u6548 JSON", response.status, "BAD_RESPONSE");
    }
    if (!response.ok) throw new ApiError(body.message || String(response.status), response.status, body.code);
    return body;
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new ApiError(error.name === "AbortError" ? "\u8BF7\u6C42\u8D85\u65F6\uFF0C\u8BF7\u7A0D\u540E\u91CD\u8BD5" : error.message);
  } finally {
    clearTimeout(timeout);
  }
}
function post(path, body = {}) {
  return api(path, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-OC-Orch-Web": "1" },
    body: JSON.stringify(body)
  });
}

// src/data.js
async function source(name, path, apply) {
  try {
    const body = await api(path);
    apply(body);
    delete state.errors[name];
    return true;
  } catch (error) {
    state.errors[name] = error.message;
    return false;
  }
}
async function refreshStatus() {
  state.statusErr = !await source("status", "/api/status", (body) => {
    state.snap = body.snapshot;
    state.statusAt = Date.now();
  });
}
async function refreshCoordinators() {
  state.coordErr = !await source("coordinators", "/api/coordinators", (body) => {
    updateCoordinators(body.coordinators || []);
    state.coordMeta = body.meta || {};
    state.coordAt = Date.now();
  });
}
async function refreshExtras() {
  const results = await Promise.all([
    source("audit", "/api/audit?limit=60", (body) => {
      state.audit = body.audit || [];
    }),
    source("inputs", "/api/inputs?limit=50", (body) => {
      state.inputs = body.inputs || [];
    }),
    source("watches", "/api/input-watches?limit=50", (body) => {
      state.watches = body.watches || [];
    }),
    source("responsibilities", "/api/responsibilities?limit=200", (body) => {
      state.resp = body.responsibilities || [];
    })
  ]);
  state.extraErr = results.some((ok) => !ok);
  if (!state.extraErr) state.extraAt = Date.now();
}
async function refreshHealth(sidOrNull) {
  const sid = sidOrNull || null;
  const path = sid ? `/api/health?session_id=${encodeURIComponent(sid)}&limit=200` : "/api/health?limit=50";
  try {
    const body = await api(path);
    if (sid) state.healthBySid[sid] = { body, at: Date.now(), error: null };
    else state.health = { body, at: Date.now(), error: null };
    state.healthErr = false;
    state.healthAt = Date.now();
    return true;
  } catch (error) {
    const failure = { error, at: Date.now() };
    if (sid) {
      const prev = state.healthBySid[sid];
      state.healthBySid[sid] = { ...failure, body: prev?.body ?? null };
    } else {
      state.health = { ...failure, body: state.health?.body ?? null };
    }
    state.healthErr = true;
    return false;
  }
}
async function refreshIncidents() {
  const ok = await source("incidents", "/api/incidents?limit=50", (body) => {
    state.incidents = body.incidents || [];
    state.incidentStatus = body.status || null;
  });
  state.incidentErr = !ok;
  return ok;
}
var tokenSeq = 0;
var detailTokens = /* @__PURE__ */ new Map();
function takeToken(key) {
  const t5 = ++tokenSeq;
  detailTokens.set(key, t5);
  return t5;
}
var isLatest = (key, token) => detailTokens.get(key) === token;
function releaseTokenIfCurrent(key, token) {
  if (isLatest(key, token)) detailTokens.delete(key);
}
async function fetchPages(first, nextPath, pages) {
  let after = null, rows = [], full = null, firstBody = null, fetched = 0;
  for (let i8 = 0; i8 < pages; i8++) {
    const path = i8 === 0 ? first : nextPath(after);
    const j3 = await api(path);
    fetched += 1;
    if (i8 === 0) firstBody = j3;
    rows = mergeRows(rows, j3.tasks || []);
    after = j3.next_after_id != null ? j3.next_after_id : null;
    full = j3.tasks_full_count ?? j3.full_count ?? full;
    if (after === null) break;
  }
  return { firstBody, rows, after, full, fetched };
}
async function fetchDetailBody(route, firstPath) {
  if (route.view === "coordinator") {
    const prev = state.coordPages[route.sid];
    const pages = Math.max(1, prev?.pages || 1);
    return fetchPages(
      firstPath,
      (after) => `/api/coordinator/${encodeURIComponent(route.sid)}?after_id=${encodeURIComponent(after)}&limit=200`,
      pages
    );
  }
  if (route.unbound) {
    const pages = Math.max(1, state.unboundPages || 1);
    return fetchPages(
      firstPath,
      (after) => `/api/tasks/unbound?limit=100&after_id=${encodeURIComponent(after)}`,
      pages
    );
  }
  return { firstBody: await api(firstPath) };
}
function maybeRestore(key, gen) {
  if (state.pendingFocus === key && routeIsCurrent(key, gen)) {
    state.pendingFocus = null;
    restoreNavigation(key);
  }
}
function loadDetail(route = parseHash()) {
  const key = routeKey(route);
  if (!needDetail(route)) return Promise.resolve();
  const gen = routeGeneration();
  const token = takeToken(key);
  state.pendingRoute.add(key);
  publish();
  const firstPath = route.unbound ? "/api/tasks/unbound?limit=100" : `/api/${route.view}/${encodeURIComponent(route.sid || route.id || "")}`;
  const previous = state.details.get(key);
  const pending = (async () => {
    try {
      const res = await fetchDetailBody(route, firstPath);
      if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return null;
      if (route.view === "coordinator") {
        state.coordPages[route.sid] = {
          tasks: res.rows,
          full: res.full ?? res.rows.length,
          after: res.after,
          truncated: res.after !== null,
          lock: false,
          pages: res.fetched
        };
      } else if (route.unbound) {
        state.unboundRows = res.rows;
        state.unboundAfter = res.after;
        state.unboundFull = res.full ?? res.rows.length;
        state.unboundTruncated = res.after !== null;
        state.unboundPages = res.fetched;
      }
      state.details.set(key, { body: res.firstBody, at: Date.now(), error: null });
      delete state.errors[key];
      return { body: res.firstBody, at: Date.now(), error: null };
    } catch (error) {
      if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return null;
      const notFound = isNotFound(error);
      state.details.set(key, {
        body: notFound ? null : previous?.body ?? null,
        at: previous?.at || 0,
        error,
        notFound
      });
      state.errors[key] = error.message;
      return { body: null, at: previous?.at || 0, error, notFound };
    } finally {
      if (isLatest(key, token)) {
        detailTokens.delete(key);
        state.pendingRoute.delete(key);
        maybeRestore(key, gen);
        publish();
      }
    }
  })();
  return pending;
}
async function refreshCoordinatorDetail(sid) {
  const key = `coordinator:${sid}`;
  const pag = state.coordPages[sid];
  if (!pag || pag.lock) return;
  const token = takeToken(key);
  const gen = routeGeneration();
  const prev = state.details.get(key);
  pag.lock = true;
  try {
    const pages = Math.max(1, pag.pages || 1);
    const { firstBody, rows, after, full } = await fetchPages(
      `/api/coordinator/${encodeURIComponent(sid)}?limit=200`,
      (after2) => `/api/coordinator/${encodeURIComponent(sid)}?after_id=${encodeURIComponent(after2)}&limit=200`,
      pages
    );
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    pag.tasks = rows;
    pag.after = after;
    pag.full = full ?? rows.length;
    pag.truncated = after !== null;
    pag.pages = pages;
    state.details.set(key, { body: firstBody, at: Date.now(), error: null });
    delete state.errors[key];
  } catch (error) {
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    const notFound = isNotFound(error);
    state.details.set(key, {
      body: notFound ? null : prev?.body ?? null,
      at: prev?.at || 0,
      error,
      notFound
    });
    state.errors[key] = error.message;
  } finally {
    pag.lock = false;
    releaseTokenIfCurrent(key, token);
  }
}
async function refreshUnbound() {
  if (state.unboundLock) return;
  const key = "tasks:unbound";
  const token = takeToken(key);
  const gen = routeGeneration();
  const prev = state.details.get(key);
  state.unboundLock = true;
  try {
    const pages = Math.max(1, state.unboundPages || 1);
    const { firstBody, rows, after, full } = await fetchPages(
      "/api/tasks/unbound?limit=100",
      (after2) => `/api/tasks/unbound?limit=100&after_id=${encodeURIComponent(after2)}`,
      pages
    );
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    state.unboundRows = rows;
    state.unboundAfter = after;
    state.unboundFull = full ?? rows.length;
    state.unboundTruncated = after !== null;
    state.unboundPages = pages;
    state.details.set(key, { body: firstBody, at: Date.now(), error: null });
    delete state.errors[key];
  } catch (error) {
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    const notFound = isNotFound(error);
    state.details.set(key, {
      body: notFound ? null : prev?.body ?? null,
      at: prev?.at || 0,
      error,
      notFound
    });
    state.errors[key] = error.message;
  } finally {
    state.unboundLock = false;
    releaseTokenIfCurrent(key, token);
  }
}
async function refreshSimpleDetail(route = parseHash()) {
  const key = routeKey(route);
  const token = takeToken(key);
  const gen = routeGeneration();
  const prev = state.details.get(key);
  try {
    const body = await api(`/api/${route.view}/${encodeURIComponent(route.id || "")}`);
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    state.details.set(key, { body, at: Date.now(), error: null });
    delete state.errors[key];
  } catch (error) {
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    const notFound = isNotFound(error);
    state.details.set(key, {
      body: notFound ? null : prev?.body ?? null,
      at: prev?.at || 0,
      error,
      notFound
    });
    state.errors[key] = error.message;
  } finally {
    releaseTokenIfCurrent(key, token);
  }
}
function refreshDetailSilent(route = parseHash()) {
  const key = routeKey(route);
  if (state.pendingRoute.has(key)) return Promise.resolve();
  if (route.view === "coordinator") {
    if (!state.coordPages[route.sid] || !state.details.get(key)?.body) return loadDetail(route);
    return refreshCoordinatorDetail(route.sid);
  }
  if (route.unbound) {
    if (!state.unboundPages || !state.details.get(key)?.body) return loadDetail(route);
    return refreshUnbound();
  }
  if (!state.details.get(key)?.body) return loadDetail(route);
  return refreshSimpleDetail(route);
}
async function loadMoreCoord(sid) {
  const key = `coordinator:${sid}`;
  if (state.pendingRoute.has(key)) return;
  const pag = state.coordPages[sid];
  if (!pag || pag.lock || !pag.after) return;
  const gen = routeGeneration();
  const token = takeToken(key);
  pag.lock = true;
  publish();
  try {
    const j3 = await api(`/api/coordinator/${encodeURIComponent(sid)}?after_id=${encodeURIComponent(pag.after)}&limit=200`);
    if (!isLatest(key, token) || !routeIsCurrent(key, gen) || state.coordPages[sid] !== pag) return;
    pag.tasks = mergeRows(pag.tasks, j3.tasks || []);
    pag.truncated = !!j3.tasks_truncated;
    pag.after = j3.next_after_id != null ? j3.next_after_id : null;
    if (j3.tasks_full_count != null) pag.full = j3.tasks_full_count;
    pag.pages += 1;
  } catch (error) {
    if (state.coordPages[sid] === pag) toast(`\u52A0\u8F7D\u5931\u8D25\uFF1A${error.message}`);
  } finally {
    releaseTokenIfCurrent(key, token);
    if (state.coordPages[sid] === pag) {
      pag.lock = false;
      publish();
    }
  }
}
async function loadMoreUnbound() {
  if (state.pendingRoute.has("tasks:unbound")) return;
  if (state.unboundLock || !state.unboundAfter) return;
  const key = "tasks:unbound";
  const gen = routeGeneration();
  const token = takeToken(key);
  state.unboundLock = true;
  publish();
  try {
    const j3 = await api(`/api/tasks/unbound?limit=100&after_id=${encodeURIComponent(state.unboundAfter)}`);
    if (!isLatest(key, token) || !routeIsCurrent(key, gen)) return;
    state.unboundRows = mergeRows(state.unboundRows, j3.tasks || []);
    state.unboundTruncated = !!j3.truncated;
    state.unboundAfter = j3.next_after_id || null;
    if (j3.full_count != null) state.unboundFull = j3.full_count;
    state.unboundPages += 1;
  } catch (error) {
    if (isLatest(key, token)) toast(`\u52A0\u8F7D\u5931\u8D25\uFF1A${error.message}`);
  } finally {
    state.unboundLock = false;
    releaseTokenIfCurrent(key, token);
    publish();
  }
}
var inFlight;
function refreshLight() {
  if (inFlight) return inFlight;
  state.refreshing = true;
  publish();
  const route = parseHash();
  inFlight = (async () => {
    try {
      const jobs = [refreshStatus(), refreshCoordinators(), refreshExtras()];
      const key = routeKey(route);
      if (route.view === "coordinator") jobs.push(refreshHealth(route.sid));
      else if (route.view === "diagnostics") jobs.push(refreshHealth(null));
      jobs.push(refreshIncidents());
      if (!state.pendingRoute.has(key) && needDetail(route)) {
        jobs.push(refreshDetailSilent(route));
      }
      await Promise.all(jobs);
    } finally {
      inFlight = null;
      state.refreshing = false;
      publish();
    }
  })();
  return inFlight;
}
async function manualRefresh() {
  await refreshLight();
  const anyErr = state.statusErr || state.coordErr || state.extraErr || Object.keys(state.errors).length > 0;
  toast(anyErr ? "\u90E8\u5206\u5237\u65B0\u5931\u8D25\uFF0C\u5DF2\u4FDD\u7559\u65E7\u6570\u636E" : "\u6570\u636E\u5DF2\u5237\u65B0");
}

// src/policy-actions.js
function rememberPolicyInput(target) {
  const form = target?.closest?.('form[data-form="delivery-policy"]');
  if (!form?.dataset.key || !target.name) return;
  const draft = { ...state.policyDrafts.get(form.dataset.key) };
  draft[target.name] = target.type === "checkbox" ? target.checked : target.value;
  state.policyDrafts.set(form.dataset.key, draft);
}
async function submitPolicy(form) {
  if (ro()) {
    toast("read-only\uFF1A\u7B56\u7565\u672A\u63D0\u4EA4");
    return;
  }
  const { scope, target, key } = form.dataset;
  const busy2 = `policy:${key}`;
  if (state.busyActions.has(busy2) || pendingPolicy(key)) return;
  if (!["task", "project"].includes(scope) || !target) return;
  const drafts = state.policyDrafts.get(key) || {};
  const val = (name) => drafts[name] ?? form.elements[name]?.value ?? "";
  if (val("confirm") !== target) {
    toast("\u786E\u8BA4\u6587\u672C\u5FC5\u987B\u9010\u5B57\u5339\u914D\u76EE\u6807");
    return;
  }
  if (!String(val("reason")).trim()) {
    toast("\u5207\u6362\u539F\u56E0\u5FC5\u586B");
    return;
  }
  const expected_revision = Number(form.dataset.revision);
  if (!Number.isSafeInteger(expected_revision) || expected_revision < 0) return;
  const inherit = scope === "task" && (drafts.inherit ?? form.elements.inherit?.checked) === true;
  const limits = {};
  for (const [name, min, max] of [["max_parallel", 1, 32], ["max_candidates", 1, 8], ["max_challenge_rounds", 0, 8]]) {
    const raw = val(name);
    if (!/^\d+$/.test(String(raw))) {
      toast("\u7B56\u7565\u4E0A\u9650\u5FC5\u987B\u662F\u6709\u9650\u6574\u6570");
      return;
    }
    const number = Number(raw);
    if (number < min || number > max) {
      toast(`\u7B56\u7565\u4E0A\u9650 ${name} \u8D85\u754C`);
      return;
    }
    limits[name] = number;
  }
  const body = {
    [scope === "task" ? "task_id" : "project"]: target,
    expected_revision,
    operation_id: `web-policy-${crypto.randomUUID()}`,
    reason: val("reason"),
    confirm: val("confirm")
  };
  if (inherit) body.inherit = true;
  else body.policy = { preset: val("preset"), stage: val("stage"), limits };
  const operation = { body, scope, target };
  try {
    savePendingPolicy(key, operation);
  } catch {
    toast("\u65E0\u6CD5\u4FDD\u5168\u539F\u64CD\u4F5C\u8EAB\u4EFD\uFF1B\u7B56\u7565\u672A\u53D1\u9001\uFF0C\u8BF7\u5148\u6062\u590D\u5F53\u524D\u7A97\u53E3\u5B58\u50A8");
    return;
  }
  state.busyActions.add(busy2);
  publish();
  try {
    const out = await post(`/api/delivery-policy/${scope}`, body);
    if (out.ok !== true || out.operation_id !== body.operation_id || out.current?.scope_id !== target || out.current?.scope !== scope || !Number.isSafeInteger(out.current?.revision)) {
      throw new Error("\u7B56\u7565\u56DE\u6267\u8EAB\u4EFD\u4E0D\u5B8C\u6574\uFF0C\u4FDD\u539F\u8BF7\u6C42\u5F85\u53EA\u8BFB\u6838\u5BF9");
    }
    clearPendingPolicy(key);
    if (state.policyDrafts.get(key) === drafts) state.policyDrafts.delete(key);
    toast(`\u7B56\u7565\u5DF2\u4FDD\u5B58\uFF1A\u5E94\u7528 v${out.applied_revision}\uFF0C\u5F53\u524D v${out.current.revision}\uFF1B${out.current.execution_enforced ? "\u4EC5\u5F71\u54CD\u540E\u7EED\u5B89\u5168\u51B3\u7B56" : "\u5C1A\u672A\u542F\u7528\u8FD0\u884C\u65F6\u5B88\u536B"}`);
    await refreshLight();
  } catch (error) {
    if (error.status >= 400 && error.status < 500 && error.code !== "BAD_RESPONSE") clearPendingPolicy(key);
    toast(`${error.message || "\u7B56\u7565\u63D0\u4EA4\u5931\u8D25"}\uFF1B\u672A\u81EA\u52A8\u91CD\u6295\u3002\u539F\u64CD\u4F5C ${body.operation_id}`);
  } finally {
    state.busyActions.delete(busy2);
    publish();
  }
}
async function recheckPolicy(scope, target) {
  const key = `${scope}:${target}`;
  const pending = pendingPolicy(key);
  if (!pending || state.busyActions.has(`policy:${key}`)) return;
  try {
    const query = scope === "task" ? "task_id" : "project";
    const current = await api(`/api/delivery-policy?${query}=${encodeURIComponent(target)}`);
    if (current.scope !== scope || current.scope_id !== target) throw new Error("\u53EA\u8BFB\u5BF9\u8D26\u8EAB\u4EFD\u4E0D\u7B26");
    if (current.version?.operation_id === pending.body.operation_id) {
      clearPendingPolicy(key);
      toast(`\u539F\u64CD\u4F5C\u5DF2\u5728\u53F0\u8D26\u786E\u8BA4\uFF0C\u5F53\u524D v${current.revision}\uFF1B\u6CA1\u6709\u91CD\u6295`);
    } else toast(`\u539F\u64CD\u4F5C\u5C1A\u672A\u80FD\u7531\u5F53\u524Dhead\u786E\u8BA4\uFF08\u53EF\u80FD\u5DF2\u6709\u540E\u7EED\u5207\u6362\uFF09\u3002\u4FDD\u7559 ${pending.body.operation_id}\uFF0C\u8BF7\u7528CLI\u5BA1\u8BA1\u6838\u5BF9\uFF0C\u7981\u6B62\u731C\u6D4B\u672A\u6267\u884C\u3002`);
    await refreshLight();
  } catch (error) {
    toast(`\u53EA\u8BFB\u5BF9\u8D26\u5931\u8D25\uFF1A${error.message}\uFF1B\u539F\u64CD\u4F5C\u4FDD\u7559`);
  }
  publish();
}

// src/operator-actions.js
var OPERATOR_TURN_STORAGE = "oc-orch.operator-turn.v1";
var records = () => {
  const value = JSON.parse(window.localStorage.getItem(OPERATOR_TURN_STORAGE) || "[]");
  if (!Array.isArray(value) || value.some((e5) => !e5?.body?.operation_id || !e5.body.project || !e5.body.session_id)) throw new Error("\u539F\u610F\u56FE\u53F0\u8D26\u635F\u574F\uFF0C\u4E0D\u731C\u6D4B/\u8986\u76D6");
  return value;
};
var save = (entries) => window.localStorage.setItem(OPERATOR_TURN_STORAGE, JSON.stringify(entries));
async function conversationAction(element, observe = false) {
  const project = element.dataset.project, sid = element.dataset.sid;
  const key = `conversation:${sid}:${project}`;
  if (ro() || !project || !sid || state.busyActions.has(key)) return;
  state.busyActions.add(key);
  publish();
  let entries, sending;
  try {
    entries = records();
    if (observe) {
      const candidates = entries.filter((e5) => e5.body.project === project && e5.body.session_id === sid);
      const operationId = prompt("\u539F\u8865\u5145\u6307\u4EE4operation ID\uFF08\u53EA\u89C2\u5BDF\uFF0C\u4E0D\u91CD\u6295\uFF09", candidates.at(-1)?.body.operation_id || "");
      if (!operationId) return;
      const scope = { project, session_id: sid, operation_id: operationId };
      const receipt = await api(`/api/coordinator/message?${new URLSearchParams(scope)}`);
      if (!receipt.operation || receipt.operation.operation_id !== operationId || receipt.operation.project !== project || receipt.operation.session_id !== sid) throw new Error("\u6CA1\u6709\u53EF\u5F52\u5C5E\u539F\u56DE\u6267\uFF1B\u672A\u77E5\u4E0D\u4EE3\u8868\u672A\u53D1\u9001");
      const result2 = await post("/api/coordinator/message/observe", scope);
      if (result2.operation?.operation_id !== operationId || result2.operation.project !== project || result2.operation.session_id !== sid) throw new Error("\u89C2\u5BDF\u56DE\u6267\u8EAB\u4EFD\u4E0D\u7B26\uFF0C\u4FDD\u7559\u539F\u610F\u56FE");
      if (result2.finished) {
        const entry = entries.find((e5) => e5.body.operation_id === operationId);
        if (entry) {
          entry.state = "finished";
          save(entries);
        }
      }
      toast(result2.finished ? "\u539F\u56DE\u5408\u7ED3\u675F\u5DF2\u6838\u9A8C\uFF1B\u663E\u5F0F\u6682\u505C/provider\u95E8\u4E0D\u53D8" : `\u4FDD\u6301\u5C4F\u969C\uFF1A${result2.reason || "\u5C1A\u65E0\u5B8C\u6574\u7ED3\u675F\u8BC1\u636E"}`);
      return;
    }
    if (entries.some((e5) => e5.body.project === project && e5.body.session_id === sid && !["finished", "refused"].includes(e5.state))) {
      throw new Error("\u5DF2\u6709\u672A\u51B3\u539F\u6307\u4EE4\uFF1B\u5148\u89C2\u5BDF\u7ED3\u675F\uFF0C\u4E0D\u6362ID\u91CD\u590D\u63D0\u4EA4");
    }
    const control = await api(`/api/coordinator/control?${new URLSearchParams({ project, session_id: sid })}`);
    if (control.read_only || control.runtime?.delivery_policy_guard_valid !== true || control.lease?.holder_session !== sid || !Number.isSafeInteger(control.session_control?.revision)) throw new Error("\u7F3A\u5C11\u5F53\u524D\u63A7\u5236/\u8D44\u6E90\u5B88\u536B\uFF0C\u672A\u53D1\u9001");
    const text = prompt("\u8865\u5145\u6307\u4EE4\uFF08\u672C\u6307\u5B9A\u5165\u53E3\u9ED8\u8BA4\u6309\u7528\u6237\u5904\u7406\uFF1B\u4E0D\u89E3\u9664\u663E\u5F0F\u6682\u505C\uFF09");
    if (!text) return;
    const confirm2 = prompt(`\u9010\u5B57\u786E\u8BA4\u9879\u76EE ${project}`);
    if (confirm2 !== project) throw new Error("\u786E\u8BA4\u4E0D\u5339\u914D\uFF0C\u672A\u53D1\u9001");
    if (entries.length >= 128) {
      const completed = entries.findIndex((e5) => ["finished", "refused"].includes(e5.state));
      if (completed < 0) throw new Error("\u672A\u51B3\u610F\u56FE\u5DF2\u8FBE\u4E0A\u9650\uFF0C\u5148\u5BF9\u8D26\uFF1B\u4E0D\u4E22\u539F\u610F\u56FE");
      entries.splice(completed, 1);
    }
    const body = {
      project,
      session_id: sid,
      operation_id: crypto.randomUUID(),
      text,
      reason: "web:explicit-user-message",
      expected_revision: control.session_control.revision,
      expected_epoch: control.lease.epoch,
      confirm: confirm2
    };
    sending = { body, state: "possible" };
    entries.push(sending);
    save(entries);
    const result = await post("/api/coordinator/message", body);
    if (result.operation?.operation_id !== body.operation_id || result.operation.project !== project || result.operation.session_id !== sid || typeof result.operation.message_id !== "string" || !["admitted", "finished"].includes(result.operation.state)) throw new Error("\u53D7\u7406\u672A\u77E5\uFF0C\u4FDD\u7559\u539FID\uFF0C\u53EA\u89C2\u5BDF\uFF0C\u4E0D\u91CD\u6295");
    sending.state = result.operation.state;
    save(entries);
    toast(`\u5DF2\u53D7\u7406 ${result.operation.message_id}\uFF1B\u53D7\u7406\u2260\u6267\u884C\u7ED3\u675F\uFF0C\u81EA\u52A8\u5F85\u529E\u5148\u7B49\u672C\u56DE\u5408\u7ED3\u675F`);
  } catch (error) {
    if (sending && error.status >= 400 && error.status < 500 && error.code !== "BAD_RESPONSE") {
      sending.state = "refused";
      try {
        save(entries);
      } catch {
      }
    }
    toast(error.message || "\u7ED3\u679C\u672A\u77E5\uFF0C\u4FDD\u7559\u539F\u610F\u56FE\uFF0C\u4E0D\u91CD\u6295");
  } finally {
    state.busyActions.delete(key);
    publish();
  }
}

// src/prototype-actions.js
function rememberPrototypeInput(target) {
  const form = target?.closest?.('form[data-form="prototype"]');
  if (form?.dataset.task && target.name) state.prototypeDrafts.set(
    form.dataset.task,
    { ...state.prototypeDrafts.get(form.dataset.task), [target.name]: target.value }
  );
}
async function submitPrototype(form) {
  const task = form.dataset.task, key = `prototype:${task}`;
  if (ro() || state.busyActions.has(key) || pendingPolicy(key)) return;
  const draft = state.prototypeDrafts.get(task) || {};
  const val = (name) => draft[name] ?? form.elements[name]?.value ?? "";
  if (val("confirm") !== task || !val("reason").trim()) {
    toast("\u987B\u66F4\u65B0\u539F\u56E0\u4E0E\u9010\u5B57Task\u786E\u8BA4\uFF1B\u672A\u63D0\u4EA4");
    return;
  }
  let plan;
  try {
    plan = JSON.parse(val("plan"));
  } catch {
    toast("\u8BA1\u5212JSON\u975E\u6CD5\uFF0C\u672A\u63D0\u4EA4");
    return;
  }
  const revision = Number(form.dataset.revision);
  if (!Number.isSafeInteger(revision) || revision < 0) return;
  const body = {
    plan,
    reason: val("reason"),
    confirm: val("confirm"),
    expected_revision: revision,
    operation_id: `web-prototype-${crypto.randomUUID()}`
  };
  try {
    savePendingPolicy(key, { scope: "prototype", target: task, body });
  } catch {
    toast("\u539F\u610F\u56FE\u6301\u4E45\u5316\u5931\u8D25\uFF0C\u96F6POST");
    return;
  }
  state.busyActions.add(key);
  publish();
  try {
    const result = await post(`/api/prototype/${encodeURIComponent(task)}`, body);
    if (result.ok !== true || result.task_id !== task || result.current?.operation_id !== body.operation_id) throw new Error("\u539F\u578B\u56DE\u6267\u8EAB\u4EFD\u672A\u77E5");
    clearPendingPolicy(key);
    if (state.prototypeDrafts.get(task) === draft) state.prototypeDrafts.delete(task);
    toast(`\u5DF2\u4FDD\u5B58\u539F\u578B\u8BB0\u5F55 v${result.revision}\uFF0C\u672A\u95ED\u73AF ${result.unresolved.length}\uFF1B\u4E0D\u662F\u4FEE\u590D\u6216\u6B63\u5F0F\u9A8C\u6536`);
    await refreshLight();
  } catch (error) {
    if (error.status >= 400 && error.status < 500 && error.code !== "BAD_RESPONSE") clearPendingPolicy(key);
    toast(`${error.message}\uFF1B\u6CA1\u6709\u81EA\u52A8\u91CD\u6295\uFF0C\u539F\u64CD\u4F5C ${body.operation_id}`);
  } finally {
    state.busyActions.delete(key);
    publish();
  }
}
async function recheckPrototype(task) {
  const key = `prototype:${task}`, pending = pendingPolicy(key);
  if (!pending || pending.invalid) return;
  try {
    const out = await api(`/api/prototype/${encodeURIComponent(task)}?operation_id=${encodeURIComponent(pending.body.operation_id)}`);
    if (out.ok !== true || out.task_id !== task || !Number.isSafeInteger(out.applied_revision)) throw new Error("\u65E0\u53EF\u5F52\u5C5E\u5386\u53F2\u56DE\u6267\uFF0C\u4E0D\u4EE3\u8868\u672A\u6267\u884C");
    clearPendingPolicy(key);
    toast(`\u539F\u64CD\u4F5C\u5E94\u7528 v${out.applied_revision}\uFF0C\u5F53\u524D v${out.revision}\uFF1B\u53EA\u8BFB\u5BF9\u8D26\uFF0C\u65E0\u91CD\u6295`);
    await refreshLight();
  } catch (error) {
    toast(error.message);
  }
  publish();
}

// src/actions.js
var RO_TOAST = "\u5F53\u524D\u4E3A\u53EA\u8BFB\u6A21\u5F0F";
async function guard(key, fn) {
  if (ro()) {
    toast(RO_TOAST);
    return;
  }
  if (state.busyActions.has(key)) return;
  state.busyActions.add(key);
  publish();
  try {
    await fn();
  } catch (error) {
    toast(error.message || "\u64CD\u4F5C\u5931\u8D25\uFF0C\u8BF7\u67E5\u770B\u6700\u65B0\u72B6\u6001");
  } finally {
    state.busyActions.delete(key);
    publish();
  }
}
function refresh() {
  try {
    refreshLight();
  } catch (_2) {
  }
}
function taskPause(id) {
  guard(`taskpause:${id}`, async () => {
    if (!confirm(`\u6682\u505C\u4EFB\u52A1 ${id}\uFF1F`)) return;
    await post(`/api/task/${encodeURIComponent(id)}/pause`, { reason: "web-pause" });
    toast("\u5DF2\u6682\u505C");
    await refreshLight();
  });
}
function taskResume(id) {
  guard(`taskresume:${id}`, async () => {
    await post(`/api/task/${encodeURIComponent(id)}/resume`, {});
    toast("\u5DF2\u6062\u590D");
    await refreshLight();
  });
}
function taskCancel(id) {
  guard(`taskcancel:${id}`, async () => {
    const reason = prompt("\u53D6\u6D88\u539F\u56E0", "web-cancel");
    if (reason === null) return;
    const c5 = prompt(`\u8F93\u5165\u4EFB\u52A1 ID \u786E\u8BA4\u53D6\u6D88\uFF1A${id}`);
    if (c5 !== id) {
      alert("\u786E\u8BA4\u4E0D\u5339\u914D");
      return;
    }
    const interrupt = confirm("\u540C\u65F6\u8BF7\u6C42\u4E2D\u65AD\u6267\u884C\u4F1A\u8BDD\uFF1F");
    await post(`/api/task/${encodeURIComponent(id)}/cancel`, { reason, confirm: c5, interrupt });
    toast("\u5DF2\u53D6\u6D88");
    await refreshLight();
  });
}
function collect(id) {
  guard(`collect:${id}`, async () => {
    if (!confirm(`\u6536\u96C6\u672C\u6B21\u5C1D\u8BD5\u7684\u7ED3\u679C\uFF1F
${id}`)) return;
    await post(`/api/attempt/${encodeURIComponent(id)}/collect`, {});
    toast("\u5DF2\u6536\u96C6");
    await refreshLight();
  });
}
var formToken = 0;
function openVerify(el) {
  if (ro()) {
    toast(RO_TOAST);
    return;
  }
  const attemptId = el.dataset.id;
  if (!attemptId) return;
  const pass = el.dataset.pass === "1";
  const key = routeKey(parseHash());
  state.verifyDrafts.delete(key);
  const entry = { attemptId, pass, token: ++formToken };
  state.verifyForms.set(key, entry);
  publish();
  requestAnimationFrame(() => {
    if (state.verifyForms.get(key) !== entry || routeKey(parseHash()) !== key) return;
    const form = document.querySelector('form[data-form="verify"]');
    if (form?.dataset.key !== key || document.activeElement?.matches?.("input,textarea,select,[contenteditable]")) return;
    form.querySelector('[name="confirm"]')?.focus({ preventScroll: true });
  });
}
function closeVerify(el) {
  const key = el?.dataset?.key || routeKey(parseHash());
  if (key && state.verifyForms.delete(key)) {
    state.verifyDrafts.delete(key);
    markDirty();
    publish();
    return;
  }
  const aid = el?.dataset?.id;
  if (aid) {
    let hit = false;
    for (const [k2, v3] of state.verifyForms) {
      if (v3.attemptId === aid) {
        state.verifyForms.delete(k2);
        hit = true;
      }
    }
    if (hit) {
      markDirty();
      publish();
    }
  }
}
function unlock(sid, project, allowProvider) {
  guard(`unlock:${sid}`, async () => {
    if (!sid) return;
    const label = allowProvider ? "\u6062\u590D\u8C03\u5EA6" : "\u89E3\u9664\u4EBA\u5DE5\u963B\u585E";
    const reason = prompt(`\u8BF7\u586B\u5199${label}\u7684\u539F\u56E0`, allowProvider ? "web-provider-recovery" : "web-manual-unlock");
    if (reason === null) return;
    const c5 = prompt(`${label} \xB7 ${sid}
\u9879\u76EE ${project || "?"}
\u8F93\u5165 UNLOCK \u786E\u8BA4`);
    if (c5 !== "UNLOCK") {
      alert("\u786E\u8BA4\u4E0D\u5339\u914D");
      return;
    }
    const payload = { project, session: sid, reason, confirm: "UNLOCK" };
    if (allowProvider) payload.allow_provider = true;
    const j3 = await post("/api/coordinator/unlock", payload);
    const r4 = j3?.result || {};
    toast(r4.cleared ? `${label} \u5B8C\u6210 ${sid}\uFF1B\u63D0\u524D ${r4.resurfaced || 0} \u6761\u5F85\u6295\u9012` : "\u6CA1\u6709\u6D3B\u52A8\u95F8\u95E8");
    await refreshLight();
  });
}
function ackNote(id) {
  guard(`acknote:${id}`, async () => {
    if (!id || !confirm(`\u786E\u8BA4\u6536\u5230\u901A\u77E5 ${id}\uFF1F`)) return;
    await post("/api/notify/ack", { ids: [id] });
    toast("\u5DF2\u786E\u8BA4\u6536\u5230");
    await refreshLight();
  });
}
function ackChecked(scopeSel, url) {
  if (ro()) {
    toast(RO_TOAST);
    return;
  }
  const ids = [...document.querySelectorAll(scopeSel)].map((c5) => c5.dataset.id).filter(Boolean);
  if (!ids.length) {
    alert("\u672A\u52FE\u9009\u4EFB\u4F55\u9879");
    return;
  }
  const lock = `ackchecked:${url}`;
  if (state.busyActions.has(lock)) return;
  const list = ids.join("\n");
  const kind = url.endsWith("/notify/ack") ? "\u901A\u77E5" : "\u544A\u8B66";
  if (!confirm(`\u786E\u8BA4\u6240\u9009\u7684 ${ids.length} \u6761${kind}\uFF1F
${list}`)) return;
  if (state.busyActions.has(lock)) return;
  state.busyActions.add(lock);
  publish();
  (async () => {
    try {
      await post(url, { ids });
      toast(`\u5DF2\u786E\u8BA4 ${ids.length} \u6761`);
      await refreshLight();
    } catch (error) {
      toast(error.message || "\u64CD\u4F5C\u5931\u8D25");
    } finally {
      state.busyActions.delete(lock);
      publish();
    }
  })();
}
function supStart() {
  guard("supstart:supervisor", async () => {
    const c5 = prompt("\u8F93\u5165 START \u786E\u8BA4\u542F\u52A8 Supervisor");
    if (c5 !== "START") {
      alert("\u786E\u8BA4\u4E0D\u5339\u914D");
      return;
    }
    await post("/api/supervisor/start", { confirm: "START" });
    toast("\u5DF2\u542F\u52A8");
    await refreshLight();
  });
}
function supStop() {
  guard("supstop:supervisor", async () => {
    if (!confirm("\u505C\u6B62 Supervisor\uFF1F\u53F0\u8D26\u4FDD\u7559\uFF0Cworker session \u4E0D\u53D7\u5F71\u54CD\u3002")) return;
    const c5 = prompt("\u8F93\u5165 STOP \u786E\u8BA4");
    if (c5 !== "STOP") {
      alert("\u786E\u8BA4\u4E0D\u5339\u914D");
      return;
    }
    await post("/api/supervisor/stop", { confirm: "STOP" });
    toast("\u5DF2\u505C\u6B62");
    await refreshLight();
  });
}
function handleManaged(el) {
  if (ro()) {
    toast(RO_TOAST);
    return;
  }
  const act = el.dataset.managed;
  const id = el.dataset.id;
  if (!act || !id) return;
  const key = `managed:${act}:${id}`;
  if (state.busyActions.has(key)) return;
  const reason = prompt(`${act} \u539F\u56E0`, "web-" + act);
  if (reason === null) return;
  const confirmText = prompt(`\u8F93\u5165\u8F93\u5165 ID \u786E\u8BA4 ${act}\uFF1A${id}`);
  if (confirmText !== id) {
    alert("\u786E\u8BA4\u4E0D\u5339\u914D");
    return;
  }
  const body = {
    owner: "web",
    confirm: id,
    expected_revision: parseInt(el.dataset.rev || "1", 10) || 1,
    reason
  };
  state.busyActions.add(key);
  publish();
  (async () => {
    try {
      if (act === "ack") body.value = confirm("\u4E1A\u52A1 ACK \u786E\u8BA4\u4E3A\u6210\u529F\uFF1F");
      await post(`/api/input/${encodeURIComponent(id)}/${act}`, body);
      toast(`${act} \u5DF2\u63D0\u4EA4`);
      await refreshLight();
    } catch (error) {
      toast(error.message || "\u64CD\u4F5C\u5931\u8D25");
    } finally {
      state.busyActions.delete(key);
      publish();
    }
  })();
}
function coordMore(sid) {
  loadMoreCoord(sid);
}
function unboundMore() {
  loadMoreUnbound();
}
function projectControl(act, el) {
  const project = el?.dataset?.project;
  const sid = el?.dataset?.sid;
  if (!project || !sid) return;
  guard(`project${act}:${sid}:${project}`, async () => {
    const label = act === "pause" ? "\u6682\u505C" : "\u7EE7\u7EED";
    const c5 = prompt(`\u8F93\u5165\u9879\u76EE\u8DEF\u5F84\u786E\u8BA4${label}\uFF1A${project}`);
    if (c5 !== project) {
      alert("\u786E\u8BA4\u4E0D\u5339\u914D");
      return;
    }
    const health = state.healthBySid[sid]?.body;
    const task = (health?.tasks || []).find((t5) => t5.project === project && t5.control);
    const control = task?.control;
    if (!control || typeof control.project_revision !== "number" || typeof control.epoch !== "number") {
      alert("\u7F3A\u5C11\u5F53\u524D\u9879\u76EE\u63A7\u5236\u7248\u672C\uFF08project_revision/epoch\uFF09\uFF0C\u64CD\u4F5C\u62D2\u7EDD\uFF1B\u8BF7\u5237\u65B0\u540E\u91CD\u8BD5");
      return;
    }
    const operationId = `web-${act}-${Date.now().toString(36)}`;
    const j3 = await post(`/api/coordinator/${act}`, {
      project,
      session_id: sid,
      operation_id: operationId,
      reason: `web \u663E\u5F0F${label}`,
      expected_revision: control.project_revision,
      expected_epoch: control.epoch,
      confirm: project
    });
    const r4 = j3?.result || {};
    toast(r4.applied === false ? `${label}\u5DF2\u5904\u7406\u4F46\u672C\u6B21\u672A\u53D8\u66F4\uFF08\u4EE5\u5F53\u524D\u72B6\u6001\u4E3A\u51C6\uFF09` : `${label}\u5DF2\u5E94\u7528\uFF1Aproject=${project} revision=${r4.revision ?? "?"}`);
    await refreshLight();
  });
}
function manual() {
  if (state.busyActions.has("manualrefresh")) return;
  state.busyActions.add("manualrefresh");
  publish();
  manualRefresh().finally(() => {
    state.busyActions.delete("manualrefresh");
    publish();
  });
}
async function controlVersion(project, sid) {
  const control = await api(`/api/coordinator/control?${new URLSearchParams({ project, session_id: sid })}`);
  if (control.read_only || control.lease?.holder_session !== sid || !Number.isSafeInteger(control.session_control?.revision) || !Number.isSafeInteger(control.lease?.epoch)) {
    throw new Error("\u7F3A\u5C11\u5F53\u524D\u63A7\u5236/\u79DF\u7EA6\u7248\u672C\uFF0C\u64CD\u4F5C\u62D2\u7EDD\uFF1B\u8BF7\u5237\u65B0\u540E\u91CD\u8BD5");
  }
  return control;
}
function checkTurnCandidate(el) {
  const project = el?.dataset?.project;
  const sid = el?.dataset?.sid;
  if (!project || !sid) return;
  guard(`checkturn:${sid}`, async () => {
    const candidate = await api(`/api/coordinator/turn-candidate?${new URLSearchParams({ project, session_id: sid })}`);
    state.turnCandidates[sid] = candidate;
    toast(candidate.eligible ? `\u53EF\u767B\u8BB0\u7528\u6237\u8F93\u5165 ${candidate.message_id}` : `\u4E0D\u53EF\u767B\u8BB0\uFF1A${candidate.reason || "unknown"}`);
    publish();
  });
}
function registerTurn(el) {
  const project = el?.dataset?.project;
  const sid = el?.dataset?.sid;
  const candidate = state.turnCandidates[sid];
  if (!project || !sid || !candidate?.eligible || !candidate.message_id) return;
  guard(`registerturn:${sid}`, async () => {
    const control = await controlVersion(project, sid);
    if (control.session_control.hold == null) throw new Error("\u5F53\u524D\u5DF2\u65E0\u56DE\u5408 hold\uFF0C\u672A\u767B\u8BB0");
    if (control.session_control.barrier != null) throw new Error("\u5DF2\u6709\u7528\u6237\u56DE\u5408\u5C4F\u969C\uFF0C\u672A\u767B\u8BB0");
    const confirmText = prompt(`\u767B\u8BB0\u7528\u6237\u8F93\u5165 ${candidate.message_id} \u5E76\u6062\u590D\u8C03\u8C10\uFF1F\u8F93\u5165\u9879\u76EE\u8DEF\u5F84\u786E\u8BA4\uFF1A${project}`);
    if (confirmText !== project) {
      toast("\u786E\u8BA4\u4E0D\u5339\u914D\uFF0C\u672A\u767B\u8BB0");
      return;
    }
    const body = {
      project,
      session_id: sid,
      operation_id: `web-register-turn-${Date.now().toString(36)}`,
      reason: "web:operator-registered-completed-turn",
      message_id: candidate.message_id,
      expected_revision: control.session_control.revision,
      expected_epoch: control.lease.epoch,
      confirm: project
    };
    const response = await post("/api/coordinator/register-turn", body);
    const result = response.result || {};
    if (response.ok !== true || result.executed !== true) {
      toast(`\u672A\u767B\u8BB0\uFF1A${response.refused || result.refused || "unknown"}\uFF08\u672A\u81EA\u52A8\u91CD\u8BD5\uFF09`);
      await refreshLight();
      return;
    }
    delete state.turnCandidates[sid];
    toast(`\u5DF2\u767B\u8BB0\u5E76\u7ED3\u675F\u7528\u6237\u56DE\u5408\uFF1A${result.reason || "ok"}\uFF1B\u4ECD\u9700\u5176\u4ED6\u95E8\u7981\u901A\u8FC7\u624D\u81EA\u52A8\u5524\u9192`);
    await refreshLight();
  });
}
function releaseHold(el) {
  const project = el?.dataset?.project;
  const sid = el?.dataset?.sid;
  if (!project || !sid) return;
  guard(`releasehold:${sid}`, async () => {
    const control = await controlVersion(project, sid);
    if (control.session_control.hold == null) {
      toast("\u5F53\u524D\u5DF2\u65E0\u56DE\u5408 hold");
      return;
    }
    if (control.session_control.barrier != null) {
      toast("\u4ECD\u6709\u7528\u6237\u56DE\u5408\u5C4F\u969C\uFF1B\u5148\u89C2\u5BDF/\u7ED3\u675F\u8BE5\u56DE\u5408\uFF0C\u4E0D\u5F3A\u5236\u89E3\u9664");
      return;
    }
    const reason = prompt("\u89E3\u9664\u539F\u56E0", "web:operator-forced-resume");
    if (!reason) return;
    const confirmText = prompt(`\u8F93\u5165\u9879\u76EE\u8DEF\u5F84\u786E\u8BA4\u5F3A\u5236\u89E3\u9664\uFF1A${project}`);
    if (confirmText !== project) {
      toast("\u786E\u8BA4\u4E0D\u5339\u914D\uFF0C\u672A\u89E3\u9664");
      return;
    }
    const token = prompt(`\u8F93\u5165 RESUME \u786E\u8BA4\u89E3\u9664 ${sid} \u7684\u56DE\u5408 hold`);
    if (token !== "RESUME") {
      toast("\u786E\u8BA4\u8BCD\u4E0D\u5339\u914D\uFF0C\u672A\u89E3\u9664");
      return;
    }
    const body = {
      project,
      session_id: sid,
      operation_id: `web-release-hold-${Date.now().toString(36)}`,
      reason,
      expected_revision: control.session_control.revision,
      expected_epoch: control.lease.epoch,
      confirm: project
    };
    const response = await post("/api/coordinator/release-hold", body);
    const result = response.result || {};
    if (response.ok !== true || result.executed !== true) {
      toast(`\u672A\u89E3\u9664\uFF1A${response.refused || result.refused || "unknown"}\uFF08\u672A\u81EA\u52A8\u91CD\u8BD5\uFF09`);
      await refreshLight();
      return;
    }
    delete state.turnCandidates[sid];
    toast(result.applied === false ? "\u5DF2\u65E0 hold\uFF08\u5E42\u7B49\uFF09\uFF0C\u672A\u6539\u5199\u63A7\u5236\u7248\u672C" : `\u5DF2\u89E3\u9664 hold\uFF1B\u63D0\u524D\u901A\u77E5 ${response.resurfaced ?? 0} \u6761\uFF0C\u4ECD\u9700\u5176\u4ED6\u95E8\u7981\u901A\u8FC7`);
    await refreshLight();
  });
}
function handleAct(el) {
  const act = el.dataset.act;
  switch (act) {
    case "taskpause":
      taskPause(el.dataset.id);
      break;
    case "taskresume":
      taskResume(el.dataset.id);
      break;
    case "taskcancel":
      taskCancel(el.dataset.id);
      break;
    case "collect":
      collect(el.dataset.id);
      break;
    case "verify":
      openVerify(el);
      break;
    case "closeverify":
      closeVerify(el);
      break;
    case "unlock":
      unlock(el.dataset.sid, el.dataset.project, el.dataset.allowProvider === "1" || el.dataset.allowProvider === "true");
      break;
    case "acknote":
      ackNote(el.dataset.id);
      break;
    case "ackchecked":
      ackChecked("#notifTable .noteck:checked", "/api/notify/ack");
      break;
    case "ackalerts":
      ackChecked("#alertTable .alck:checked", "/api/alerts/ack");
      break;
    case "supstart":
      supStart();
      break;
    case "supstop":
      supStop();
      break;
    case "coordmore":
      coordMore(el.dataset.sid);
      break;
    case "unboundmore":
      unboundMore();
      break;
    case "projpause":
      projectControl("pause", el);
      break;
    case "projresume":
      projectControl("resume", el);
      break;
    case "usersend":
      conversationAction(el);
      break;
    case "userobserve":
      conversationAction(el, true);
      break;
    case "checkturn":
      checkTurnCandidate(el);
      break;
    case "registerturn":
      registerTurn(el);
      break;
    case "releasehold":
      releaseHold(el);
      break;
    case "prototypefeature":
      state.prototypeSelected.set(el.dataset.task, el.dataset.feature);
      publish();
      break;
    case "prototyperecheck":
      recheckPrototype(el.dataset.task);
      break;
    case "manualrefresh":
      manual();
      break;
    case "policyrecheck":
      recheckPolicy(el.dataset.scope, el.dataset.target);
      break;
    default:
      break;
  }
}
function handleTab(el) {
  const value = el.dataset.tab;
  if (!value) return;
  const host = el.dataset.tabHost || "coordinator";
  if (state.tab[host] === value) return;
  state.tab[host] = value;
  publish();
}
async function submitVerify(form) {
  if (ro()) {
    toast(RO_TOAST);
    return;
  }
  const attemptId = form.dataset.id;
  const pass = form.dataset.pass === "1";
  const key = form.dataset.key || routeKey(parseHash());
  const entryAtSubmit = state.verifyForms.get(key);
  if (!entryAtSubmit) return;
  const drafts = state.verifyDrafts.get(key) || {};
  const valueOf = (name) => {
    const draftValue = drafts[name];
    if (typeof draftValue === "string") return draftValue;
    return form.elements?.[name]?.value ?? "";
  };
  const reason = valueOf("reason").trim();
  if (!pass && !reason) {
    alert("\u9A8C\u6536\u5931\u8D25\u9700\u8981\u539F\u56E0");
    return;
  }
  if (valueOf("confirm") !== attemptId) {
    alert("\u786E\u8BA4\u4E0D\u5339\u914D");
    return;
  }
  const bkey = `verify:${attemptId}`;
  if (state.busyActions.has(bkey)) return;
  state.busyActions.add(bkey);
  publish();
  try {
    await post(`/api/attempt/${encodeURIComponent(attemptId)}/verify`, {
      pass,
      confirm: attemptId,
      evidence: valueOf("evidence").split(";").map((s5) => s5.trim()).filter(Boolean),
      notes: valueOf("notes"),
      reason,
      force: false,
      // 沿用旧后端：不宣称 force/人工例外
      run_checks: !!form.elements?.run_checks?.checked
    });
    toast("\u9A8C\u6536\u5DF2\u63D0\u4EA4");
    const current = state.verifyForms.get(key);
    if (current && current.token === entryAtSubmit.token) {
      state.verifyForms.delete(key);
      state.verifyDrafts.delete(key);
    }
    publish();
    refresh();
  } catch (error) {
    toast(error.message || "\u9A8C\u6536\u63D0\u4EA4\u5931\u8D25");
    markDirty();
  } finally {
    state.busyActions.delete(bkey);
    publish();
  }
}
function sendInput(form) {
  if (ro()) {
    toast(RO_TOAST);
    return;
  }
  const fd = new FormData(form);
  const body = {};
  const submitted = {};
  for (const k2 of ["session", "intent", "text", "owner", "request_id", "project", "task", "attempt", "target_session"]) {
    const v3 = String(fd.get(k2) || "");
    submitted[k2] = v3;
    if (k2 === "text") {
      body.text = v3;
    } else if (v3.trim()) {
      body[k2] = v3.trim();
    }
  }
  if (!body.session || !body.intent || !body.text?.trim()) {
    alert("session / intent / text \u5FC5\u586B");
    return;
  }
  const key = `send:${body.session}:${body.request_id || body.intent}`;
  if (state.busyActions.has(key)) return;
  state.busyActions.add(key);
  publish();
  (async () => {
    try {
      await post("/api/input/send", body);
      toast("\u6307\u4EE4\u5DF2\u53D7\u7406\uFF0C\u53EF\u5728\u8BB0\u5F55\u4E2D\u67E5\u770B\u6295\u9012\u8FDB\u5EA6\u3002");
      if (form.isConnected) {
        const cur = new FormData(form);
        let unchanged = true;
        for (const [k2, v3] of Object.entries(submitted)) {
          if (String(cur.get(k2) || "") !== v3) {
            unchanged = false;
            break;
          }
        }
        if (unchanged) form.reset();
      }
      await refreshLight();
    } catch (error) {
      toast(error.message || "\u53D1\u9001\u5931\u8D25");
    } finally {
      state.busyActions.delete(key);
      publish();
    }
  })();
}
function handleClick(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;
  const copyEl = target.closest?.("[data-copy]");
  if (copyEl) {
    copyText(copyEl.dataset.copy || "");
    return;
  }
  const tabEl = target.closest?.("[data-tab]");
  if (tabEl) {
    handleTab(tabEl);
    return;
  }
  const managedEl = target.closest?.("[data-managed]");
  if (managedEl) {
    handleManaged(managedEl);
    return;
  }
  const actEl = target.closest?.("[data-act]");
  if (actEl) {
    handleAct(actEl);
  }
}
async function submitRework(form) {
  if (ro()) {
    toast(RO_TOAST);
    return;
  }
  const id = form.dataset.id;
  const fd = new FormData(form);
  const feedback = String(fd.get("feedback") || "").trim();
  const confirm2 = String(fd.get("confirm") || "");
  const work_mode = String(fd.get("work_mode") || "latency-first");
  if (!id || !feedback) {
    toast("\u8FD4\u5DE5\u53CD\u9988\u4E0D\u80FD\u4E3A\u7A7A");
    return;
  }
  if (confirm2 !== id) {
    toast("\u786E\u8BA4\u4E0D\u5339\u914D\uFF0C\u672A\u8FD4\u5DE5");
    return;
  }
  guard(`rework:${id}`, async () => {
    const response = await post(
      `/api/task/${encodeURIComponent(id)}/rework`,
      { feedback, work_mode, confirm: confirm2 }
    );
    const result = response.result || {};
    toast(`\u5DF2\u521B\u5EFA\u8FD4\u5DE5\u5C1D\u8BD5 ${result.attempt_id || "?"} g${result.generation ?? "?"}\uFF0C\u6A21\u5F0F ${work_mode}`);
    await refreshLight();
  });
}
function handleSubmit(event) {
  const form = event.target;
  if (!form || form.tagName !== "FORM") return;
  const kind = form.dataset.form;
  if (kind === "verify") {
    event.preventDefault();
    submitVerify(form);
  } else if (kind === "rework") {
    event.preventDefault();
    submitRework(form);
  } else if (kind === "send") {
    event.preventDefault();
    sendInput(form);
  } else if (kind === "delivery-policy") {
    event.preventDefault();
    submitPolicy(form);
  } else if (kind === "prototype") {
    event.preventDefault();
    submitPrototype(form);
  }
}
function handleInput(event) {
  const target = event.target;
  rememberPolicyInput(target);
  rememberPrototypeInput(target);
  if (target?.id === "taskSearch") {
    state.search = target.value;
    publish();
  }
  const verifyForm = target?.closest?.('form[data-form="verify"]');
  if (verifyForm?.dataset?.key && target?.name && typeof target.value === "string") {
    const key = verifyForm.dataset.key;
    const drafts = state.verifyDrafts.get(key) || {};
    drafts[target.name] = target.value;
    state.verifyDrafts.set(key, drafts);
  }
  if (target?.closest?.("form[data-form]")) state.dirtyForm = true;
}
function handleChange(event) {
  const target = event.target;
  rememberPolicyInput(target);
  rememberPrototypeInput(target);
  if (target?.id === "auto") {
    state.auto = target.checked;
    publish();
    return;
  }
  if (target?.matches?.("form[data-form]")) state.dirtyForm = true;
}

// src/app.js
var VIEW_LABEL = {
  overview: "\u8C03\u5EA6\u603B\u89C8",
  coordinator: "\u8C03\u5EA6\u8005\u5DE5\u4F5C\u53F0",
  task: "\u4EFB\u52A1\u8BE6\u60C5",
  attempt: "\u5C1D\u8BD5\u8BE6\u60C5",
  tasks: "\u5168\u5C40\u4EFB\u52A1",
  diagnostics: "\u7CFB\u7EDF\u8BCA\u65AD"
};
var OcOrchApp = class extends i4 {
  // light DOM：共享 /assets/app.css，不使用 shadow，导航/健康/自动刷新由 Lit 管理。
  createRenderRoot() {
    return this;
  }
  connectedCallback() {
    super.connectedCallback();
    const session = { alive: true };
    this._lifecycle = session;
    this._onClick = (e5) => handleClick(e5);
    this._onSubmit = (e5) => handleSubmit(e5);
    this._onInput = (e5) => handleInput(e5);
    this._onChange = (e5) => handleChange(e5);
    this.addEventListener("click", this._onClick);
    this.addEventListener("submit", this._onSubmit);
    this.addEventListener("input", this._onInput);
    this.addEventListener("change", this._onChange);
    this._unsub = subscribe(() => {
      if (this.isConnected && this._lifecycle === session) this.requestUpdate();
    });
    this._onHash = () => {
      if (this._lifecycle === session) this.handleRoute();
    };
    window.addEventListener("hashchange", this._onHash);
    this.boot(session);
  }
  disconnectedCallback() {
    super.disconnectedCallback();
    const session = this._lifecycle;
    this._lifecycle = null;
    if (session) session.alive = false;
    clearInterval(this._ticker);
    this._ticker = null;
    if (this._onHash) window.removeEventListener("hashchange", this._onHash);
    this._onHash = null;
    if (this._onClick) {
      this.removeEventListener("click", this._onClick);
      this.removeEventListener("submit", this._onSubmit);
      this.removeEventListener("input", this._onInput);
      this.removeEventListener("change", this._onChange);
    }
    this._onClick = this._onSubmit = this._onInput = this._onChange = null;
    this._unsub?.();
    this._unsub = null;
  }
  async boot(session) {
    if (!session.alive || this._lifecycle !== session) return;
    state.ready = false;
    await Promise.all([refreshStatus(), refreshCoordinators(), refreshExtras()]);
    if (!session.alive || this._lifecycle !== session) return;
    state.ready = true;
    this.handleRoute();
    this._ticker = setInterval(() => this.tick(), 5e3);
  }
  tick() {
    if (!state.auto || document.hidden || !this._lifecycle) return;
    refreshLight();
  }
  handleRoute() {
    const route = parseHash();
    const key = routeKey(route);
    const changed = key !== state.currentKey;
    navigate();
    if (changed) {
      if (needDetail(route)) {
        const entry = state.details.get(key);
        if (entry?.body && !state.pendingRoute.has(key)) {
          restoreNavigation(key);
        } else {
          state.pendingFocus = key;
        }
      } else {
        restoreNavigation(key);
      }
    }
    if (needDetail(route)) {
      loadDetail(route).catch(() => {
      });
    }
    refreshLight();
  }
  skipToMain(event) {
    event.preventDefault();
    document.getElementById("main")?.focus();
  }
  render() {
    const route = parseHash();
    return b2`
      <a class="skip" href="#main" @click=${this.skipToMain}>跳到主内容</a>
      <header>${this.headerTemplate(route)}</header>
      <main id="main" tabindex="-1">
        ${this.routeView(route)}
      </main>
      <div id="toast" class="toast" role="status" aria-live="polite"></div>
    `;
  }
  // 视图级 DOM 缓存：cache 以外层每类视图的稳定模板字面量为键（不同 view 模板 literal），
  // 页面切换走 cache 冻结/恢复，同视图后台刷新同模板原地更新（不替换 DOM、不丢焦点/滚动）。
  // keyed 在 cache 内部以 routeKey 为键做跨对象隔离：同视图不同对象 A→B 重建子树，
  // 避免对象 A 的表单/展开状态落到对象 B。
  routeView(route) {
    const key = routeKey(route);
    switch (route.view) {
      case "overview":
        return h4(b2`<div id="overviewRoot" aria-live="polite">${i7(key, this.overviewBody(route))}</div>`);
      case "coordinator":
        return h4(b2`<div id="coordRoot" aria-live="polite">${i7(key, this.coordinatorBody(route))}</div>`);
      case "task":
        return h4(b2`<div id="taskRoot" aria-live="polite">${i7(key, this.taskBody(route))}</div>`);
      case "attempt":
        return h4(b2`<div id="attemptRoot" aria-live="polite">${i7(key, this.attemptBody(route))}</div>`);
      case "tasks":
        return h4(b2`<div id="tasksRoot" aria-live="polite">${i7(key, this.tasksBody(route))}</div>`);
      case "diagnostics":
        return h4(b2`<section id="view-diagnostics" aria-label="系统诊断">${i7(key, this.diagnosticsBody(route))}</section>`);
      default:
        return b2`<div class="attn rank-mid"><span>未知视图：${route.view}</span></div>`;
    }
  }
  overviewBody(route) {
    if (!state.snap && !state.statusErr) return loadingPanel(VIEW_LABEL.overview);
    return b2`${errorBanner(state.statusErr || state.coordErr || state.extraErr)}${renderOverview()}`;
  }
  coordinatorBody(route) {
    return this.detailBody(route, (body) => renderCoordinator(route.sid, body));
  }
  taskBody(route) {
    return this.detailBody(route, (body) => renderTask(route.id, body));
  }
  attemptBody(route) {
    return this.detailBody(route, (body) => renderAttempt(route.id, body));
  }
  tasksBody(route) {
    if (route.unbound) return this.detailBody(route, () => renderUnboundTasks());
    if (!state.snap && !state.statusErr) return loadingPanel(VIEW_LABEL.tasks);
    return b2`${errorBanner(state.statusErr)}${renderTasksView()}`;
  }
  diagnosticsBody(route) {
    if (!state.snap && !state.statusErr) return loadingPanel(VIEW_LABEL.diagnostics);
    return b2`${errorBanner(state.extraErr || state.statusErr || route.section === "responsibilities" && state.coordErr)}${renderDiagnostics(route.section)}`;
  }
  detailBody(route, renderFn) {
    const key = routeKey(route);
    const entry = state.details.get(key);
    const pending = state.pendingRoute.has(key);
    if (!state.ready) return loadingPanel(VIEW_LABEL[route.view] || "\u8BE6\u60C5");
    if (pending && !entry?.body) return loadingPanel(VIEW_LABEL[route.view] || "\u8BE6\u60C5");
    if (entry?.notFound) return missingPanel(route);
    if (!entry?.body) {
      return loadFailedPanel(entry?.error);
    }
    const banner = entry?.error ? staleBanner(entry) : A;
    return b2`${banner}${renderFn(entry.body)}`;
  }
  headerTemplate(route) {
    const navId = ["tasks", "task", "attempt"].includes(route.view) ? "tasks" : route.view === "diagnostics" ? "diagnostics" : "overview";
    return b2`
      <h1><a class="brand" href="#/" aria-label="返回调度总览"><span class="brand-mark" aria-hidden="true"></span>oc-orch</a></h1>
      <nav id="nav" aria-label="主导航">
        <a href="#/" data-nav="overview" aria-current=${navId === "overview" ? "page" : A}>总览</a>
        <a href="#/tasks" data-nav="tasks" aria-current=${navId === "tasks" ? "page" : A}>全局任务</a>
        <a href="#/diagnostics" data-nav="diagnostics" aria-current=${route.view === "diagnostics" ? "page" : A}>系统诊断</a>
      </nav>
      <div class="header-tools">
      ${this.healthTemplate()}
      <label class="inline" title="5 秒自动刷新"><input type="checkbox" id="auto" ?checked=${state.auto}> 自动刷新</label>
      <button id="refreshBtn" data-act="manualrefresh" ?disabled=${state.refreshing || state.busyActions.has("manualrefresh")}>
        ${state.refreshing ? "\u5237\u65B0\u4E2D\u2026" : "\u5237\u65B0"}
      </button></div>
    `;
  }
  healthTemplate() {
    const s5 = state.snap?.supervisor || {};
    const svc = state.snap?.service || {};
    const process = processStatus(s5, state.statusErr);
    const anyErr = state.statusErr || state.coordErr || state.extraErr || Object.keys(state.errors).length > 0;
    return b2`<details id="health">
      <summary><span class=${`presence ${process.tone}`}>调度服务${process.text}</span>
        ${anyErr ? b2`<span class="warn">部分数据过期</span>` : A}
        ${ro() ? b2`<span class="badge">只读</span>` : A}</summary>
      <dl class="kv health-detail">
        <dt>状态更新</dt><dd>${state.statusAt ? fmtAge(Date.now() - state.statusAt) + "\u524D" : "\u5C1A\u672A\u8BFB\u53D6"}</dd>
        <dt>会话更新</dt><dd>${state.coordAt ? fmtAge(Date.now() - state.coordAt) + "\u524D" : "\u5C1A\u672A\u8BFB\u53D6"}</dd>
        <dt>进程 ID</dt><dd>${s5.pid ?? "\u672A\u77E5"}</dd>
        <dt>事件连接</dt><dd>${connectionStatus(s5, state.statusErr)}</dd>
        ${svc.base ? b2`<dt>服务地址</dt><dd>${svc.base} ${svc.version || ""}</dd>` : A}
      </dl></details>`;
  }
};
function loadingPanel(title) {
  return b2`<h2>${title || "\u52A0\u8F7D\u4E2D\u2026"}</h2><div class="muted" role="status">正在从服务端读取数据…</div>`;
}
function errorBanner(flag) {
  return flag ? b2`<div class="attn rank-mid"><span class="warn">部分数据刷新失败，显示内容可能过期。</span> <button data-act="manualrefresh">重试</button></div>` : A;
}
function staleBanner(entry) {
  return b2`<div class="attn rank-mid"><span class="warn">详情刷新失败，当前显示缓存的旧内容：${entry.error?.message || ""} <button data-act="manualrefresh">重试</button></span></div>`;
}
function loadFailedPanel(error) {
  return b2`<h2>加载失败</h2>
    <div class="attn rank-high"><span>无法读取详情：${error?.message || "\u7F51\u7EDC\u6216\u670D\u52A1\u9519\u8BEF"} <button data-act="manualrefresh">重试</button> <a href="#/">返回总览</a></span></div>`;
}
function missingPanel(route) {
  const text = route.view === "coordinator" ? `\u5F53\u524D\u5217\u8868\u4E2D\u627E\u4E0D\u5230\u8BE5\u4F1A\u8BDD\uFF1A${route.sid}` : `\u627E\u4E0D\u5230\u8BE5\u8BB0\u5F55\uFF1A${route.id}`;
  return b2`<h2>${VIEW_LABEL[route.view] || "\u8BE6\u60C5"}</h2>
    <div class="attn rank-high"><span>${text}。<a href="#/">返回总览</a> · <a href="#/tasks">全局任务</a></span></div>`;
}
customElements.define("oc-orch-app", OcOrchApp);
export {
  OcOrchApp
};
/*! Bundled license information:

@lit/reactive-element/css-tag.js:
  (**
   * @license
   * Copyright 2019 Google LLC
   * SPDX-License-Identifier: BSD-3-Clause
   *)

@lit/reactive-element/reactive-element.js:
lit-html/lit-html.js:
lit-element/lit-element.js:
lit-html/directive.js:
lit-html/directives/cache.js:
lit-html/directives/repeat.js:
  (**
   * @license
   * Copyright 2017 Google LLC
   * SPDX-License-Identifier: BSD-3-Clause
   *)

lit-html/is-server.js:
  (**
   * @license
   * Copyright 2022 Google LLC
   * SPDX-License-Identifier: BSD-3-Clause
   *)

lit-html/directive-helpers.js:
  (**
   * @license
   * Copyright 2020 Google LLC
   * SPDX-License-Identifier: BSD-3-Clause
   *)

lit-html/directives/keyed.js:
  (**
   * @license
   * Copyright 2021 Google LLC
   * SPDX-License-Identifier: BSD-3-Clause
   *)
*/
