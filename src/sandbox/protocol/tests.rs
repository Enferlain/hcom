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
            target: "worker-b".into(),
            target_generation: None,
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
    assert!(Message::new(old.id, payload).is_err());
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
