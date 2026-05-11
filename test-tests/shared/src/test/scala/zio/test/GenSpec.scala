      } yield x == 2 * y
      assertZIO(provideSize(result)(100))(isTrue)
    },
    test("suspend lazily constructs a generator") {
      check(genIntList)(as => assert(as.reverse.reverse)(equalTo(as)))
    },
    test("runCollect") {
      val domain = List.range(-10, 10)
      val gen    = Gen.fromIterable(domain)
      for {
        a <- gen.runCollect
        b <- gen.runCollect
      } yield assert(a)(equalTo(domain)) &&
        assert(b)(equalTo(domain))
    } @@ scala2Only,
    test("runCollectN") {
      val gen = Gen.int(-10, 10)
      for {
        a <- gen.runCollectN(100)
        b <- gen.runCollectN(100)
      } yield assert(a)(not(equalTo(b))) &&
        assert(a)(hasSize(equalTo(100))) &&
        assert(b)(hasSize(equalTo(100)))
    },
    test("runCollectN does not freeze random values when fromIterable is sequenced after a random generator") {
      val deterministic = Gen.fromIterable(LazyList.iterate(0)(_ + 1))
      val randomFirst = for {
        id <- Gen.uuid
        _  <- deterministic
      } yield id
      val deterministicFirst = for {
        _  <- deterministic
        id <- Gen.uuid
      } yield id

      for {
        randomFirstSamples        <- randomFirst.runCollectN(20)
        deterministicFirstSamples <- deterministicFirst.runCollectN(20)
      } yield assert(randomFirstSamples.distinct)(hasSize(isGreaterThan(1))) &&
        assert(deterministicFirstSamples.distinct)(hasSize(isGreaterThan(1)))
    },
    test("runHead") {
      assertZIO(Gen.int(-10, 10).runHead)(isSome(isWithin(-10, 10)))
    },
    test("collectAll") {
      val gen = Gen.collectAll(
        List(
          Gen.fromIterable(List(1, 2)),
          Gen.fromIterable(List(3)),
          Gen.fromIterable(List(4, 5))
        )
      )
      assertZIO(gen.runCollect)(
        equalTo(
          List(
            List(1, 3, 4),
            List(1, 3, 5),
            List(2, 3, 4),
            List(2, 3, 5)
          )
        )
      )
    },
    test("unfoldGen") {
      sealed trait Command
      case object Pop                   extends Command
      final case class Push(value: Int) extends Command

      val genPop: Gen[Any, Command] = Gen.const(Pop)

      def genPush: Gen[Any, Command] = Gen.int.map(value => Push(value))

      val genCommands: Gen[Any, List[Command]] =
        Gen.unfoldGen(0) { n =>
          if (n <= 0)
            genPush.map(command => (n + 1, command))
          else
            Gen.oneOf(
              genPop.map(command => (n - 1, command)),
              genPush.map(command => (n + 1, command))
            )