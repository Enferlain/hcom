use super::*;

fn hello_body() -> FrameBody {
    FrameBody::Hello {
        max_frame_bytes: MAX_ENCODED_FRAME,
        directory: None,
    }
}

fn fixture() -> (Binding, Message) {
    let worker = Origin {
        worker: "worker-a".into(),
        generation: "generation-a".into(),
        incarnation: Uuid::new_v4().to_string(),
    };
    let message = Message::new(
        MessageId {
            origin: worker.clone(),
            sequence: 1,
        },
        MessagePayload {
            conversation: None,
            target: "worker-b".into(),
            target_generation: None,
            sender_routing_generation: None,
            broadcast: false,
            thread: Some("work".into()),
            reply_to: None,
            intent: Intent::Inform,
            text: "test".into(),
        },
    )
    .unwrap();
    let binding = Binding {
        team: "team".into(),
        sandbox: "sandbox-a".into(),
        worker,
    };
    (binding, message)
}

fn raw_frame(frame: &Frame) -> Vec<u8> {
    let data = serde_json::to_vec(frame).unwrap();
    let mut bytes = (data.len() as u32).to_be_bytes().to_vec();
    bytes.extend(data);
    bytes
}

#[test]
fn recipient_generation_is_in_the_digest_and_checked_against_delivery_binding() {
    let (binding, old) = fixture();
    let json =
        br#"{"target":"worker-b","thread":"work","reply_to":null,"intent":"inform","text":"test"}"#;
    assert_eq!(serde_json::to_vec(&old.payload).unwrap(), json);
    assert_eq!(
        serde_json::from_slice::<MessagePayload>(json).unwrap(),
        old.payload
    );
    old.validate().unwrap();

    let mut payload = old.payload.clone();
    payload.target = binding.worker.worker.clone();
    payload.target_generation = Some(binding.worker.generation.clone());
    let good = Message::new(old.id.clone(), payload.clone()).unwrap();
    validate_message_size(Receiver::Guest, &binding, &good).unwrap();

    payload.target_generation = Some("retired".into());
    let stale = Message::new(old.id.clone(), payload.clone()).unwrap();
    assert_ne!(stale.digest, good.digest);
    assert!(validate_message_size(Receiver::Guest, &binding, &stale).is_err());
    payload.target_generation = Some("bad generation".into());
    assert!(Message::new(old.id.clone(), payload).is_err());
    let mut forged = old.payload;
    forged.sender_routing_generation = Some("fake-host-route".into());
    let forged = Message::new(old.id, forged).unwrap();
    assert!(validate_message_size(Receiver::Host, &binding, &forged).is_err());
}

#[test]
fn directory_handshake_is_host_only_and_rejects_duplicate_targets() {
    let (binding, _) = fixture();
    let entry = TeamPeer {
        name: "host".into(),
        alias: None,
        generation: "host-generation".into(),
    };
    let mut frame = Frame {
        version: VERSION,
        epoch: 1,
        binding: binding.clone(),
        body: FrameBody::Hello {
            max_frame_bytes: MAX_ENCODED_FRAME,
            directory: Some(vec![entry.clone()]),
        },
    };
    assert!(
        Connection::new(binding.clone(), 1, Receiver::Guest)
            .unwrap()
            .accept(frame.clone(), 1)
            .is_ok()
    );
    assert!(
        Connection::new(binding.clone(), 1, Receiver::Host)
            .unwrap()
            .accept(frame.clone(), 1)
            .is_err()
    );
    frame.body = FrameBody::Hello {
        max_frame_bytes: MAX_ENCODED_FRAME,
        directory: Some(vec![entry.clone(), entry]),
    };
    assert!(write_frame(&mut Vec::new(), &frame).is_err());
    frame.body = FrameBody::Hello {
        max_frame_bytes: MAX_ENCODED_FRAME,
        directory: Some(
            (0..=MAX_DIRECTORY_PEERS)
                .map(|index| TeamPeer {
                    name: format!("peer_{index}"),
                    alias: None,
                    generation: "generation".into(),
                })
                .collect(),
        ),
    };
    assert!(
        write_frame(&mut Vec::new(), &frame)
            .unwrap_err()
            .to_string()
            .contains("peer limit")
    );
}

#[test]
fn rejection_receipts_are_host_only_and_bound_to_the_guest_origin() {
    let (binding, message) = fixture();
    let hello = Frame {
        version: VERSION,
        epoch: 1,
        binding: binding.clone(),
        body: hello_body(),
    };
    let body = FrameBody::Rejected {
        id: message.id.clone(),
        digest: message.digest,
    };
    let frame = Frame {
        body: body.clone(),
        ..hello.clone()
    };
    let mut guest = Connection::new(binding.clone(), 1, Receiver::Guest).unwrap();
    guest.accept(hello.clone(), 1).unwrap();
    assert_eq!(guest.accept(frame.clone(), 1).unwrap(), body);

    let mut host = Connection::new(binding, 1, Receiver::Host).unwrap();
    host.accept(hello, 1).unwrap();
    assert!(host.accept(frame.clone(), 1).is_err());

    let mut forged = frame;
    let mut other = message.id;
    other.origin.generation = "other".into();
    forged.body = FrameBody::Rejected {
        id: other,
        digest: message.digest,
    };
    assert!(write_frame(&mut Vec::new(), &forged).is_err());
}

#[test]
fn length_is_checked_before_reading_body_and_eof_is_distinct() {
    for length in [0, MAX_ENCODED_FRAME - HEADER_LEN + 1, u32::MAX] {
        let mut input = length.to_be_bytes().to_vec();
        input.extend(b"unread");
        let mut reader = input.as_slice();
        assert!(
            read_frame(&mut reader)
                .unwrap_err()
                .to_string()
                .contains("length")
        );
        assert_eq!(reader, b"unread");
    }
    assert!(read_frame(&mut &[][..]).unwrap().is_none());
    assert!(read_frame(&mut &[0, 0][..]).is_err());
    assert!(read_frame(&mut &[0, 0, 0, 2, b'{'][..]).is_err());
    assert!(read_frame(&mut &[0, 0, 0, 1, 0xff][..]).is_err());
}

#[test]
fn accepts_serial_frames_only_after_hello_and_fences_stale_epoch() {
    let (binding, message) = fixture();
    let mut frame = Frame {
        version: VERSION,
        epoch: 3,
        binding: binding.clone(),
        body: hello_body(),
    };
    let mut wire = Vec::new();
    write_frame(&mut wire, &frame).unwrap();
    frame.body = FrameBody::Send(message.clone());
    write_frame(&mut wire, &frame).unwrap();
    assert!(
        Connection::new(binding.clone(), 3, Receiver::Host)
            .unwrap()
            .receive(&mut raw_frame(&frame).as_slice(), 3)
            .is_err()
    );
    let mut connection = Connection::new(binding, 3, Receiver::Host).unwrap();
    let mut wire = wire.as_slice();
    assert_eq!(
        connection.receive(&mut wire, 3).unwrap(),
        Some(hello_body())
    );
    assert_eq!(
        connection.receive(&mut wire, 3).unwrap(),
        Some(FrameBody::Send(message))
    );
    let bytes = raw_frame(&frame);
    let mut wire = bytes.as_slice();
    assert!(
        connection
            .receive(&mut wire, 4)
            .unwrap_err()
            .to_string()
            .contains("stale")
    );
    assert_eq!(wire, bytes.as_slice());
    assert!(
        connection
            .receive(&mut wire, 3)
            .unwrap_err()
            .to_string()
            .contains("closed")
    );
}

#[test]
fn invalid_frames_cannot_begin_or_change_a_binding() {
    let (binding, message) = fixture();
    let hello = Frame {
        version: VERSION,
        epoch: 2,
        binding: binding.clone(),
        body: hello_body(),
    };
    let mut incorrect = hello.clone();
    incorrect.version += 1;
    assert!(write_frame(&mut Vec::new(), &incorrect).is_err());
    assert!(
        Connection::new(binding.clone(), 2, Receiver::Host)
            .unwrap()
            .receive(&mut raw_frame(&incorrect).as_slice(), 2)
            .is_err()
    );
    incorrect = hello.clone();
    incorrect.body = FrameBody::Hello {
        max_frame_bytes: MAX_ENCODED_FRAME / 2,
        directory: None,
    };
    assert!(
        Connection::new(binding.clone(), 2, Receiver::Host)
            .unwrap()
            .receive(&mut raw_frame(&incorrect).as_slice(), 2)
            .unwrap_err()
            .to_string()
            .contains("frame limit")
    );

    let check_after_hello = |frame: &Frame| {
        let mut connection = Connection::new(binding.clone(), 2, Receiver::Host).unwrap();
        connection
            .receive(&mut raw_frame(&hello).as_slice(), 2)
            .unwrap();
        assert!(
            connection
                .receive(&mut raw_frame(frame).as_slice(), 2)
                .is_err()
        );
    };
    check_after_hello(&hello);
    incorrect = hello.clone();
    let mut addressed_payload = message.payload.clone();
    addressed_payload.target = binding.worker.worker.clone();
    let addressed = Message::new(message.id.clone(), addressed_payload).unwrap();
    incorrect.body = FrameBody::Deliver(addressed.clone());
    check_after_hello(&incorrect);
    incorrect.body = FrameBody::Send(message.clone());
    incorrect.binding.team = "other".into();
    check_after_hello(&incorrect);
    incorrect.binding = binding.clone();
    incorrect.epoch += 1;
    check_after_hello(&incorrect);

    let mut forged = message.clone();
    forged.id.origin.worker = "imposter".into();
    incorrect.epoch = 2;
    incorrect.body = FrameBody::Send(forged);
    check_after_hello(&incorrect);
    let mut changed = message.clone();
    changed.payload.text.push('!');
    incorrect.body = FrameBody::Send(changed);
    check_after_hello(&incorrect);

    let mut guest = Connection::new(binding.clone(), 2, Receiver::Guest).unwrap();
    guest.receive(&mut raw_frame(&hello).as_slice(), 2).unwrap();
    incorrect.body = FrameBody::Send(message.clone());
    assert!(
        guest
            .receive(&mut raw_frame(&incorrect).as_slice(), 2)
            .unwrap_err()
            .to_string()
            .contains("direction")
    );
    let mut guest = Connection::new(binding.clone(), 2, Receiver::Guest).unwrap();
    guest.receive(&mut raw_frame(&hello).as_slice(), 2).unwrap();
    incorrect.body = FrameBody::Deliver(addressed);
    assert_eq!(
        guest
            .receive(&mut raw_frame(&incorrect).as_slice(), 2)
            .unwrap(),
        Some(incorrect.body.clone())
    );
    incorrect.body = FrameBody::Deliver(message.clone());
    assert!(
        guest
            .receive(&mut raw_frame(&incorrect).as_slice(), 2)
            .unwrap_err()
            .to_string()
            .contains("target mismatch")
    );

    let mut json = serde_json::to_value(&hello).unwrap();
    json["body"]["kind"] = serde_json::json!("unknown");
    assert!(serde_json::from_value::<Frame>(json).is_err());
    let mut oversized = message.payload;
    oversized.text = "x".repeat(MAX_ENCODED_FRAME as usize);
    let oversized = Message::new(message.id, oversized).unwrap();
    assert!(validate_message_size(Receiver::Host, &binding, &oversized).is_err());
}

#[test]
fn guest_wire_schema_has_no_control_events_or_host_metadata() {
    use serde_json::{Value, json};

    let (binding, message) = fixture();
    let frame = Frame {
        version: VERSION,
        epoch: 1,
        binding: binding.clone(),
        body: FrameBody::Send(message),
    };
    let hello = Frame {
        body: hello_body(),
        ..frame.clone()
    };
    let check_rejected = |data: Value, diagnostic: &str| {
        let data = serde_json::to_vec(&data).unwrap();
        let mut bytes = (data.len() as u32).to_be_bytes().to_vec();
        bytes.extend(data);
        let mut host = Connection::new(binding.clone(), 1, Receiver::Host).unwrap();
        host.receive(&mut raw_frame(&hello).as_slice(), 1).unwrap();
        let error = host.receive(&mut bytes.as_slice(), 1).unwrap_err();
        assert!(format!("{error:#}").contains(diagnostic), "{error:#}");
    };
    for kind in [
        "launch",
        "kill",
        "configure",
        "terminal",
        "sql",
        "read",
        "events",
        "system",
    ] {
        let mut data = serde_json::to_value(&frame).unwrap();
        data["body"] = json!({"kind": kind, "body": {}});
        check_rejected(data, "unknown variant");
    }
    for (field, value) in [
        ("pid", json!(1)),
        ("transcript_path", json!("/fixture/transcript")),
        ("notify_endpoint", json!("fixture")),
        ("_relay", json!(true)),
        ("_sandbox", json!({"team": "other"})),
        ("sender_kind", json!("system")),
    ] {
        let mut data = serde_json::to_value(&frame).unwrap();
        data["body"]["body"]["payload"][field] = value;
        check_rejected(data, "unknown field");
    }
}

#[test]
fn interrupted_header_can_be_retried() {
    let (binding, _) = fixture();
    let frame = Frame {
        version: VERSION,
        epoch: 1,
        binding,
        body: hello_body(),
    };
    let wire = raw_frame(&frame);
    let mut input = InterruptedOnce {
        bytes: &wire,
        interrupted: false,
    };
    assert_eq!(read_frame(&mut input).unwrap(), Some(frame));
    assert!(input.interrupted);
}

struct InterruptedOnce<'a> {
    bytes: &'a [u8],
    interrupted: bool,
}

impl Read for InterruptedOnce<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if !std::mem::replace(&mut self.interrupted, true) {
            return Err(ErrorKind::Interrupted.into());
        }
        self.bytes.read(buffer)
    }
}

#[test]
fn validated_messages_reject_invalid_routing_identifiers() {
    let (_, message) = fixture();
    for invalid in ["", "has space", "contains\u{1b}control", "é"] {
        let mut payload = message.payload.clone();
        payload.target = invalid.into();
        assert!(Message::new(message.id.clone(), payload).is_err());
        let mut payload = message.payload.clone();
        payload.thread = Some(invalid.into());
        assert!(Message::new(message.id.clone(), payload).is_err());
    }
}

#[test]
fn broadcast_messages_reject_targets_threads_replies_and_preserve_legacy_digests() {
    let (binding, legacy) = fixture();
    let legacy_json =
        br#"{"target":"worker-b","thread":"work","reply_to":null,"intent":"inform","text":"test"}"#;
    assert_eq!(serde_json::to_vec(&legacy.payload).unwrap(), legacy_json);

    let broadcast = Message::new(
        legacy.id.clone(),
        MessagePayload {
            conversation: None,
            target: String::new(),
            target_generation: None,
            sender_routing_generation: None,
            broadcast: true,
            thread: None,
            reply_to: None,
            intent: Intent::Inform,
            text: "team update".into(),
        },
    )
    .unwrap();
    let other_binding = Binding {
        sandbox: "sandbox-b".into(),
        worker: Origin {
            worker: "worker-b".into(),
            generation: "generation-b".into(),
            incarnation: binding.worker.incarnation.clone(),
        },
        ..binding.clone()
    };
    validate_message_size(Receiver::Host, &binding, &broadcast).unwrap();
    validate_message_size(Receiver::Guest, &other_binding, &broadcast).unwrap();

    let hello = Frame {
        version: VERSION,
        epoch: 1,
        binding: binding.clone(),
        body: hello_body(),
    };
    let mut host = Connection::new(binding.clone(), 1, Receiver::Host).unwrap();
    host.accept(hello.clone(), 1).unwrap();
    assert!(
        host.accept(
            Frame {
                body: FrameBody::Deliver(broadcast.clone()),
                ..hello.clone()
            },
            1
        )
        .is_err()
    );
    let mut guest = Connection::new(other_binding.clone(), 1, Receiver::Guest).unwrap();
    guest
        .accept(
            Frame {
                binding: other_binding.clone(),
                ..hello
            },
            1,
        )
        .unwrap();
    let mut forged_send = broadcast.clone();
    forged_send.id.origin = other_binding.worker.clone();
    let forged_send = Message::new(forged_send.id, forged_send.payload).unwrap();
    assert!(
        guest
            .accept(
                Frame {
                    version: VERSION,
                    epoch: 1,
                    binding: other_binding,
                    body: FrameBody::Send(forged_send),
                },
                1
            )
            .is_err()
    );

    let invalid_cases = [
        MessagePayload {
            conversation: None,
            target: "worker-b".into(),
            ..broadcast.payload.clone()
        },
        MessagePayload {
            conversation: None,
            target_generation: Some("generation-b".into()),
            ..broadcast.payload.clone()
        },
        MessagePayload {
            conversation: None,
            thread: Some("work".into()),
            ..broadcast.payload.clone()
        },
        MessagePayload {
            conversation: None,
            reply_to: Some(legacy.id.clone()),
            ..broadcast.payload.clone()
        },
    ];
    for payload in invalid_cases {
        assert!(Message::new(legacy.id.clone(), payload).is_err());
    }
    let mut forged_sender_gen = broadcast.payload;
    forged_sender_gen.sender_routing_generation = Some("fake".into());
    let forged_sender_gen = Message::new(legacy.id, forged_sender_gen).unwrap();
    assert!(validate_message_size(Receiver::Host, &binding, &forged_sender_gen).is_err());
}

/// The conversation field is canonical-omitted: ungranted payloads digest
/// exactly like retained legacy messages, granted payloads digest with the
/// opaque id, and legacy payloads still parse.
#[test]
fn conversation_field_is_canonical_omitted_and_digest_stable() {
    let (binding, legacy) = fixture();
    let ungranted = serde_json::to_vec(&legacy.payload).unwrap();
    assert!(!String::from_utf8_lossy(&ungranted).contains("conversation"));

    let granted_payload = MessagePayload {
        conversation: Some("conv-1".into()),
        target_generation: Some("generation-b".into()),
        sender_routing_generation: Some("host-recipient".into()),
        ..legacy.payload.clone()
    };
    let granted = Message::new(legacy.id.clone(), granted_payload).unwrap();
    let granted_json = serde_json::to_vec(&granted.payload).unwrap();
    assert!(String::from_utf8_lossy(&granted_json).contains(r#""conversation":"conv-1""#));
    assert_ne!(granted.digest, legacy.digest);

    let parsed: MessagePayload = serde_json::from_str(
        r#"{"target":"worker-b","thread":"work","reply_to":null,"intent":"inform","text":"test"}"#,
    )
    .unwrap();
    assert_eq!(parsed, legacy.payload);
    assert_eq!(parsed.conversation, None);
    let _ = &binding;
}

/// Scoped payloads take exactly one strict form: a fully addressed host
/// grant delivery or a guest expansion request with no target and no
/// generation, both always labeled, neither carrying a reply. Malformed
/// mixtures are rejected at construction, before any digest or import.
#[test]
fn scoped_payloads_take_only_the_two_strict_forms() {
    let (binding, legacy) = fixture();
    let scoped = |thread: Option<&str>,
                  target: Option<&str>,
                  target_generation: Option<&str>,
                  routing: Option<&str>,
                  reply: Option<MessageId>| {
        Message::new(
            legacy.id.clone(),
            MessagePayload {
                conversation: Some("conv-1".into()),
                thread: thread.map(str::to_owned),
                target: target.map(str::to_owned).unwrap_or_default(),
                target_generation: target_generation.map(str::to_owned),
                sender_routing_generation: routing.map(str::to_owned),
                reply_to: reply,
                ..legacy.payload.clone()
            },
        )
    };
    let parent = Some(legacy.id.clone());
    // Host grant delivery: addressed target plus both generations.
    assert!(
        scoped(
            Some("work"),
            Some("worker-b"),
            Some("generation-b"),
            Some("host-recipient"),
            None
        )
        .is_ok()
    );
    // Guest expansion request: no target, no generation.
    let expansion = scoped(Some("work"), None, None, None, None).unwrap();
    let ack = MessagePayload {
        intent: Intent::Ack,
        ..expansion.payload
    };
    assert!(Message::new(legacy.id.clone(), ack).is_err());

    // Malformed mixtures and omissions.
    assert!(
        scoped(
            None,
            Some("worker-b"),
            Some("generation-b"),
            Some("host-recipient"),
            None
        )
        .is_err()
    );
    assert!(
        scoped(
            Some("work"),
            None,
            Some("generation-b"),
            Some("host-recipient"),
            None
        )
        .is_err()
    );
    assert!(scoped(Some("work"), None, None, Some("host-recipient"), None).is_err());
    assert!(scoped(Some("work"), Some("worker-b"), None, None, None).is_err());
    assert!(
        scoped(
            Some("work"),
            Some("worker-b"),
            Some("generation-b"),
            None,
            None
        )
        .is_err()
    );
    assert!(
        scoped(
            Some("work"),
            Some("worker-b"),
            Some("generation-b"),
            Some("host-recipient"),
            parent.clone()
        )
        .is_err()
    );
    assert!(scoped(Some("work"), None, None, None, parent).is_err());
    assert!(
        scoped(
            Some("work"),
            Some("../evil"),
            Some("generation-b"),
            Some("host-recipient"),
            None
        )
        .is_err()
    );
    assert!(
        scoped(
            Some("../evil"),
            Some("worker-b"),
            Some("generation-b"),
            Some("host-recipient"),
            None
        )
        .is_err()
    );

    let broadcast = MessagePayload {
        conversation: Some("conv-1".into()),
        broadcast: true,
        target: String::new(),
        ..legacy.payload.clone()
    };
    assert!(Message::new(legacy.id.clone(), broadcast).is_err());
    let _ = &binding;
}

/// Guest Send frames carry a conversation id only as an expansion request;
/// an addressed grant claim is a forged grant and the same scoped message is
/// a valid host Deliver frame.
#[test]
fn guest_send_frames_carry_only_expansion_shapes() {
    let (binding, legacy) = fixture();
    let scoped = Message::new(
        legacy.id.clone(),
        MessagePayload {
            conversation: Some("conv-1".into()),
            target_generation: Some("generation-b".into()),
            sender_routing_generation: Some("host-recipient".into()),
            ..legacy.payload.clone()
        },
    )
    .unwrap();
    let frame = |body| Frame {
        version: VERSION,
        epoch: 1,
        binding: binding.clone(),
        body,
    };
    // A fully scoped message is rejected on the guest Send direction by the
    // routing-generation rule before routing ever sees it.
    let failure = frame(FrameBody::Send(scoped.clone()))
        .validate()
        .unwrap_err()
        .to_string();
    assert!(
        failure.contains("guest cannot set a sandbox sender routing generation"),
        "unexpected rejection: {failure}"
    );
    // The same scoped message is a valid host Deliver frame for its target.
    assert!(
        frame(FrameBody::Deliver(scoped.clone()))
            .validate()
            .is_err()
    );
    let deliver_binding = Binding {
        team: binding.team.clone(),
        sandbox: "sandbox-b".into(),
        worker: Origin {
            worker: "worker-b".into(),
            generation: "generation-b".into(),
            incarnation: binding.worker.incarnation.clone(),
        },
    };
    Frame {
        version: VERSION,
        epoch: 1,
        binding: deliver_binding,
        body: FrameBody::Deliver(scoped),
    }
    .validate()
    .unwrap();

    // The Send direction also rejects a bare conversation marker that is
    // neither valid form, independent of the payload shape rules.
    let raw = Message {
        digest: [0; 32],
        id: legacy.id.clone(),
        payload: MessagePayload {
            conversation: Some("conv-1".into()),
            ..legacy.payload.clone()
        },
    };
    let failure = frame(FrameBody::Send(raw))
        .validate()
        .unwrap_err()
        .to_string();
    assert!(
        failure.contains("guest thread expansion requests carry no target or generation"),
        "unexpected rejection: {failure}"
    );

    // A genuine expansion request is a valid guest Send frame, and the same
    // verbatim request is a valid host Deliver frame for a grant-holding
    // destination whose grant check happens in its inbox.
    let expansion = Message::new(
        legacy.id.clone(),
        MessagePayload {
            conversation: Some("conv-1".into()),
            target: String::new(),
            target_generation: None,
            sender_routing_generation: None,
            broadcast: false,
            thread: Some("work".into()),
            reply_to: None,
            intent: Intent::Request,
            text: "expanding".into(),
        },
    )
    .unwrap();
    frame(FrameBody::Send(expansion.clone()))
        .validate()
        .unwrap();
    let deliver_binding = Binding {
        team: binding.team.clone(),
        sandbox: "sandbox-b".into(),
        worker: Origin {
            worker: "worker-b".into(),
            generation: "generation-b".into(),
            incarnation: binding.worker.incarnation.clone(),
        },
    };
    Frame {
        version: VERSION,
        epoch: 1,
        binding: deliver_binding,
        body: FrameBody::Deliver(expansion),
    }
    .validate()
    .unwrap();
}
