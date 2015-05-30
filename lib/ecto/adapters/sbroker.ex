defmodule Ecto.Adapters.SBroker do

  alias Ecto.Adapters.SBroker.Worker

  @doc """
  Starts pool of connections for the given connection module and options.
  """
  def start_link(conn_mod, opts) do
    import Supervisor.Spec
    {pool_opts, conn_opts} = split_opts(opts)

    name = Keyword.fetch!(pool_opts, :name)
    broker = worker(:sbroker, [{:local, name}, __MODULE__, pool_opts])

    size = Keyword.fetch!(pool_opts, :size)
    workers = for id <- 1..size do
      worker(Worker, [name, conn_mod, conn_opts], [id: id])
    end
    worker_sup_opts = [strategy: :one_for_one, max_restarts: size]
    worker_sup = supervisor(Supervisor, [workers, worker_sup_opts])


    children = [broker, worker_sup]
    sup_opts = [strategy: :rest_for_one, name: Module.concat(name, Supervisor)]
    Supervisor.start_link(children, sup_opts)
  end

  @doc """
  Stop the pool.
  """
  def stop(pool) do
    pid = Process.whereis(pool)
    ref = Process.monitor(pid)
    Proces.exit(pid, :shutdown)
    receive do
      {:DOWN, ^ref, _, _, :shutdown} ->
        :ok
      {:DOWN, ^ref, _, _, reason} ->
        exit({reason, {__MODULE__, :stop, [pool]}})
    after
      5000 ->
        Process.exit(pid, :kill)
        receive do
          {:DOWN, ^ref, _, _, :shutdown} ->
            :ok
          {:DOWN, ^ref, _, _, reason} ->
            exit({reason, {__MODULE__, :stop, [pool]}})
        end
    end
  end

  @doc """
  Checkout a worker from the pool.
  """
  def checkout(pool, timeout) do
    case :sbroker.ask(pool) do
      {:go, ref, pid, _, _} ->
        {Worker, {pid, ref}}
      {:drop, _} ->
        exit({:drop, {__MODULE__, :checkout, [pool, timeout]}})
    end
  end

  @doc """
  Checkin a worker to the pool.
  """
  def checkin(_, Worker, worker) do
    Worker.done(worker)
  end

  @doc false
  def async_ask_r(pool, tag) do
    :sbroker.async_ask_r(pool, tag)
  end

  @doc false
  def cancel_or_await(pool, tag) do
    case :sbroker.cancel(pool, tag) do
      1     -> :cancelled
      false -> :sbroker.await(tag, 0)
    end
  end

  @doc false
  def init(opts) do
    client_queue = Keyword.fetch!(opts, :queue)
    worker_out = Keyword.fetch!(opts, :worker_out)
    worker_queue = {:squeue_naive, :undefined, worker_out, :infinity, :drop}
    interval = Keyword.fetch!(opts, :interval)
    {:ok, {client_queue, worker_queue, interval}}
  end

  ## Helpers

  defp split_opts(opts) do
    pool_keys = [:name, :size, :queue, :interval]
    {pool_opts, conn_opts} = Keyword.split(opts, pool_keys)

    conn_opts = conn_opts
      |> Keyword.put(:timeout, Keyword.get(conn_opts, :connect_timeout, 5000))

    timeout = Keyword.fetch!(conn_opts, :timeout) * 1_000

    pool_opts = pool_opts
      |> Keyword.put_new(:size, 10)
      |> Keyword.put_new(:queue, {:squeue_timeout, timeout, :out, :infinity, :drop})
      |> Keyword.put_new(:worker_out, :out_r)
      |> Keyword.put_new(:interval, 100)

    {pool_opts, conn_opts}
  end
end
