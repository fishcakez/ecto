defmodule Ecto.Adapters.Poolboy do

  alias Ecto.Adapters.Worker

  @doc """
  Starts pool of connections for the given connection module and options.
  """
  def start_link(conn_mod, opts) do
    {pool_opts, conn_opts} = split_opts(opts)
    :poolboy.start_link(pool_opts, {conn_mod, conn_opts})
  end

  @doc """
  Stop the pool.
  """
  def stop(pool) do
    :poolboy.stop(pool)
  end

  @doc """
  Checkout a worker from the pool.
  """
  def checkout(pool, timeout) do
    {Worker, :poolboy.checkout(pool, true, timeout)}
  end

  @doc """
  Checkin a worker to the pool.
  """
  def checkin(pool, Worker, worker) do
    :poolboy.checkin(pool, worker)
  end

  ## Helpers

  defp split_opts(opts) do
    {pool_opts, conn_opts} = Keyword.split(opts, [:name, :size, :max_overflow])

    {pool_name, pool_opts} = Keyword.pop(pool_opts, :name)

    pool_opts = pool_opts
      |> Keyword.put_new(:size, 10)
      |> Keyword.put_new(:max_overflow, 0)

    pool_opts =
      [name: {:local, pool_name},
       worker_module: Worker] ++ pool_opts

    {pool_opts, conn_opts}
  end
end
