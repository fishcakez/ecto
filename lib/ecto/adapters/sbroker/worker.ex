defmodule Ecto.Adapters.SBroker.Worker do

  require Logger
  alias Ecto.Adapters.SBroker

  @type modconn :: {module :: atom, conn :: pid}

  @doc """
  Starts a linked worker for the given module and params.
  """
  def start_link(pool, module, params) do
    Connection.start_link(__MODULE__, {pool, module, params})
  end

  @doc """
  Asks for the module and the underlying connection process.
  """
  @spec ask({pid, reference}, timeout) :: {:ok, modconn} | {:error, Exception.t}
  def ask({pid, reference}, timeout) do
    Connection.call(pid, {:ask, reference}, timeout)
  end

  @doc """
  Asks for the module and the underlying connection process.
  """
  @spec ask!({pid, reference}, timeout) :: modconn | no_return
  def ask!(worker, timeout) do
    case ask(worker, timeout) do
      {:ok, modconn} -> modconn
      {:error, err}  -> raise err
    end
  end

  @doc """
  Opens a transaction.

  Invoked when the client wants to open up a connection.

  The worker process starts to monitor the caller and
  will wipeout all connection state in case of crashes.

  It returns an `:ok` tuple if the transaction can be
  opened, a `:sandbox` tuple in case the transaction
  could not be openned because it is in sandbox mode
  or an `:error` tuple, usually when the adapter is
  unable to connect.

  ## FAQ

  Question: What happens if `open_transaction/2` is
  called when a transaction is already open?

  Answer: If a transaction is already open, the previous
  transaction along side its connection will be discarded
  and a new one will be started transparently. The reasoning
  is that if the client is calling `open_transaction/2` when
  one is already open, is because the client lost its state,
  and we should treat it transparently by disconnecting the
  old state and starting a new one.
  """
  @spec open_transaction({pid, reference}, timeout) ::
    {:ok, Ecto.Adapters.Worker.modconn} | {:sandbox, modconn} | {:error, Exception.t}
  def open_transaction({pid, ref}, timeout) do
    Connection.call(pid, {:open_transaction, ref}, timeout)
  end

  @doc """
  Closes a transaction.

  Both sandbox and open transactions can be closed.
  Returns `:not_open` if a transaction was not open.
  """
  @spec close_transaction({pid, reference}, timeout) :: :not_open | :closed
  def close_transaction({pid, ref}, timeout) do
    Connection.call(pid, {:close_transaction, ref}, timeout)
  end

  @doc """
  Breaks a transaction.

  Automatically forces the worker to disconnect unless
  in sandbox mode. Returns `:not_open` if a transaction
  was not open.
  """
  @spec break_transaction({pid, reference}, timeout) ::
    :broken | :not_open | :sandbox
  def break_transaction({pid, ref}, timeout) do
    Connection.call(pid, {:break_transaction, ref}, timeout)
  end

  @doc """
  Starts a sandbox transaction.

  A sandbox transaction is not monitored by the worker.
  This functions returns an `:ok` tuple in case a sandbox
  transaction has started, a `:sandbox` tuple if it was
  already in sandbox mode or `:already_open` if it was
  previously open.
  """
  @spec sandbox_transaction({pid, reference}, timeout) ::
    {:ok, modconn} | {:sandbox, modconn} | :already_open
  def sandbox_transaction({pid, ref}, timeout) do
    Connection.call(pid, {:sandbox_transaction, ref}, timeout)
  end

  @doc false
  def done({pid, ref}) do
    Connection.cast(pid, {:done, ref})
  end

  ## Callbacks

  def init({pool, module, params}) do
    _ = Process.flag(:trap_exit, true)
    tag = make_ref()
    backoff = :backoff.type(:backoff.init(100, 5000), :jitter)
    state = %{conn: nil, params: params, transaction: :closed, module: module,
              pool: pool, tag: tag, ref: nil, client: nil, backoff: backoff}
    {:connect, :connect, state}
  end

  def connect(_, %{ref: nil, client: nil, conn: nil, module: module,
  params: params, pool: pool, tag: tag, backoff: backoff} = s) do
    case module.connect(params) do
      {:ok, conn} ->
        SBroker.async_ask_r(pool, tag)
        {_, backoff} = :backoff.succeed(backoff)
        {:ok, %{s | conn: conn, backoff: backoff}}
      {:error, err} ->
        Logger.error(fn() ->
          [inspect(pool), " worker could not connect: " |
            Exception.message(err)]
        end)
        {delay, backoff} = :backoff.fail(backoff)
        {:backoff, delay, %{s | backoff: backoff}}
    end
  end

  def disconnect({:EXIT, _},
  %{client: nil, ref: nil, pool: pool, tag: tag} = s) do
    case SBroker.cancel_or_await(pool, tag) do
      :cancelled ->
        {:connect, :reconnect, %{s | conn: nil, trnasaction: :closed}}
      {:go, ref, pid, _, _} ->
        client = Process.monitor(pid)
        s = %{s | client: client, ref: ref, conn: nil, transaction: :closed}
        {:noconnect, :reconnect, s}
      {:drop, _} ->
        {:connect, :reconnect, %{s | conn: nil, transaction: :closed}}
    end
  end
  def disconnect({:EXIT, _}, %{client: client, ref: ref} = s)
  when is_reference(client) and is_reference(ref) do
    {:noconnect, %{s | conn: nil, transaction: :closed}}
  end
  def disconnect({:DOWN, _}, %{module: module, conn: conn} = s) do
    conn && module.disconnect(conn)
    s = %{s | client: nil, ref: nil, conn: nil, transaction: :closed}
    {:connect, :reconnect, s}
  end
  def disconnect(:drop, %{client: nil, ref: nil, module: module, conn: conn} = s) do
    module.disconnect(conn)
    {:connect, :reconnect, %{s | conn: nil, transaction: :closed}}
  end
  def disconnect(:break_transaction, %{module: module, conn: conn} = s) do
    module.disconnect(conn)
    {:noconnect, %{s | conn: nil, transaction: :closed}}
  end

  def handle_call({cmd, ref}, _, %{ref: ref, conn: conn} = s)
  when is_reference(ref) and is_pid(conn) do
    handle(cmd, s)
  end
  def handle_call({:ask, ref}, _, %{ref: ref, conn: nil} = s)
  when is_reference(ref) do
    {:reply, {:error, :noconnect}, s}
  end
  def handle_call({trans, ref}, _, %{ref: ref, conn: nil} = s)
  when trans in [:break_transaction, :close_transaction, :open_transaction,
  :sandbox_transaction] and is_reference(ref) do
    {:reply, :noconnect, s}
  end

  ## Cast

  def handle_cast({:done, ref},
  %{ref: ref, client: client, conn: conn, pool: pool, tag: tag} = s)
  when is_reference(ref) and is_pid(conn) do
    Process.demonitor(client, [:flush])
    SBroker.async_ask_r(pool, tag)
    {:noreply, %{s | ref: nil, client: nil}}
  end
  def handle_cast({:done, ref}, %{ref: ref, client: client, conn: nil} = s)
  when is_reference(ref) do
    Process.demonitor(client, [:flush])
    {:connect, :reconnect, %{s | ref: nil, client: nil}}
  end

  ## Info

  def handle_info({tag, {:go, ref, pid, _, _}}, %{ref: nil, tag: tag} = s) do
    client = Process.monitor(pid)
    {:noreply, %{s | ref: ref, client: client}}
  end
  def handle_info({tag, {:drop, _}}, %{ref: nil, tag: tag} = s) do
    {:disconnect, :drop, s}
  end
  def handle_info({:EXIT, conn, reason}, %{conn: conn} = s) when is_pid(conn) do
    {:disconnect, {:EXIT, reason}, s}
  end
  def handle_info({:DOWN, ref, _, _, reason}, %{client: ref} = s) do
    {:dsconnect, {:DOWN, reason}, s}
  end
  def handle_info(_, s) do
    {:noreply, s}
  end

  def terminate(_, %{conn: conn, module: module}) do
    conn && module.disconnect(conn)
  end

  ## Helpers

  ## Break transaction

  defp handle(:break_transaction, %{transaction: :sandbox} = s) do
    {:reply, :sandbox, s}
  end
  defp handle(:break_transaction, %{transaction: :closed} = s) do
    {:reply, :not_open, s}
  end
  defp handle(:break_transaction, s) do
    {:disconnect, :break_transaction, :broken, s}
  end

  ## Close transaction

  defp handle(:close_transaction, %{transaction: :sandbox} = s) do
    {:reply, :closed, %{s | transaction: :closed}}
  end
  defp handle(:close_transaction, %{transaction: :closed} = s) do
    {:reply, :not_open, s}
  end
  defp handle(:close_transaction, %{transaction: :open} = s) do
    {:reply, :closed, %{s | transaction: :closed}}
  end

  ## Ask

  defp handle(:ask, s) do
    {:reply, {:ok, modconn(s)}, s}
  end

  ## Open transaction

  defp handle(:open_transaction, %{transaction: :sandbox} = s) do
    {:reply, {:sandbox, modconn(s)}, s}
  end
  defp handle(:open_transaction, %{transaction: :closed} = s) do
    {:reply, {:ok, modconn(s)}, %{s | transaction: :open}}
  end
  defp handle(:open_transaction, %{transaction: :open} = s) do
    {:reply, :already_open, s}
  end

  ## Sandbox transaction

  defp handle(:sandbox_transaction, %{transaction: :sandbox} = s) do
    {:reply, {:sandbox, modconn(s)}, s}
  end
  defp handle(:sandbox_transaction, %{transaction: :closed} = s) do
    {:reply, {:ok, modconn(s)}, %{s | transaction: :sandbox}}
  end
  defp handle(:sandbox_transaction, %{transaction: :open} = s) do
    {:reply, :already_open, s}
  end

  defp modconn(%{conn: conn, module: module}) do
    {module, conn}
  end
end
