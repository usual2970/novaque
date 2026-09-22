// novaque admin poller (KTD10): vanilla JS, no dependencies, no build step.
// Pages are fully usable with JS disabled (links + plain forms only); this
// only refreshes live backlog counts. BASE is injected by the layout as the
// mount prefix, so every URL is built from the same single constant the
// server templates use.
(function () {
	"use strict";

	var main = document.querySelector("main[data-poll]");
	if (!main || typeof fetch !== "function") {
		return;
	}

	var endpoint = main.getAttribute("data-poll");
	var timer = null;
	var FIELDS = ["pending", "ready", "in_flight", "dead"];

	function setText(el, value) {
		if (el) {
			el.textContent = String(value);
		}
	}

	function applyCounts(prefix, id, counts) {
		if (!counts) {
			return;
		}
		for (var i = 0; i < FIELDS.length; i++) {
			var key = FIELDS[i];
			setText(document.querySelector("[" + prefix + '="' + id + "-" + key + '"]'), counts[key]);
		}
	}

	function apply(data) {
		if (!data) {
			return;
		}
		if (data.topics) {
			// /api/summary: dashboard rows.
			for (var i = 0; i < data.topics.length; i++) {
				var topic = data.topics[i];
				applyCounts("data-t", topic.id, topic.totals);
				var channels = topic.channels || [];
				for (var j = 0; j < channels.length; j++) {
					applyCounts("data-b", channels[j].id, channels[j].backlog);
				}
			}
		} else if (data.channels) {
			// /api/topics/{id}: topic detail rows.
			applyCounts("data-t", data.id, data.totals);
			for (var k = 0; k < data.channels.length; k++) {
				applyCounts("data-b", data.channels[k].id, data.channels[k].backlog);
			}
		} else if (data.id) {
			// /api/channels/{id}: channel detail box.
			applyCounts("data-b", data.id, data.backlog);
		}
	}

	function poll() {
		fetch(BASE + endpoint, { headers: { Accept: "application/json" } })
			.then(function (res) {
				return res.ok ? res.json() : null;
			})
			.then(function (data) {
				apply(data);
			})
			.catch(function () {
				// Swallowed: the next tick retries; a failed poll must not
				// take the page down.
			})
			.then(schedule);
	}

	function schedule() {
		timer = setTimeout(poll, 5000);
	}

	// Gated on document.hidden: hidden tabs stop polling and resume with an
	// immediate refresh on visibilitychange.
	document.addEventListener("visibilitychange", function () {
		if (document.hidden) {
			if (timer) {
				clearTimeout(timer);
				timer = null;
			}
		} else if (!timer) {
			poll();
		}
	});

	poll();
})();
